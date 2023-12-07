import logging
import re

import pandas as pd
import requests
import zipfile
from io import BytesIO

from pandas import DataFrame
from pydantic import BaseModel, AnyHttpUrl
from typing import List, Any
from tqdm import tqdm
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.datatypes import ExternalID
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_login import Login

import config

logging.basicConfig(level=config.loglevel)
logger = logging.getLogger(__name__)


class Form(BaseModel):
    form: str
    id: str


class ZipFileHandler(BaseModel):
    zip_file_path: AnyHttpUrl
    zip_content: Any = BytesIO()
    forms: List[Form] = []
    unzipped_content: Any = BytesIO()
    lexeme_ids: List[str] = list()
    no_dannet_lexeme_ids: List[str] = list()
    df: DataFrame = DataFrame()
    wbi: WikibaseIntegrator = None

    # login: Login = None

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.setup_wbi()
        self.download_zip()
        self.unzip_content()
        # self.print_head_unzipped_content()
        self.extract_forms()
        self.create_dataframe_and_export_csv()
        self.fetch_danish_lexeme_ids()
        self.fetch_danish_lexeme_ids_without_dannet_property()
        self.find_dannet_ids_for_lexemes_and_upload()

    def download_zip(self) -> None:
        response = requests.get(url=self.zip_file_path, stream=True)
        if response.status_code != 200:
            raise ValueError("Invalid URL or unable to download the file.")

        total_size = int(response.headers.get('content-length', 0))

        with tqdm(total=total_size, unit='B', unit_scale=True, desc='Downloading') as pbar:
            for chunk in response.iter_content(chunk_size=1024):
                self.zip_content.write(chunk)
                pbar.update(len(chunk))

    def unzip_content(self) -> None:
        print("Unzipping content")
        self.zip_content.seek(0)
        self.unzipped_content = BytesIO()

        with zipfile.ZipFile(self.zip_content, 'r') as zip_ref:
            words_owl_files = [file_name for file_name in zip_ref.namelist() if file_name.endswith('words.rdf')]
            if not words_owl_files:
                raise ValueError("No 'words.owl' file found in the zip archive.")

            words_owl_file = words_owl_files[0]
            self.unzipped_content.write(zip_ref.read(words_owl_file))
            self.unzipped_content.seek(0)

    @property
    def unzipped_data_exists(self) -> bool:
        return bool(self.unzipped_content)

    def print_head_unzipped_content(self) -> None:
        print("Printing head of unzipped content")
        self.check_unzipped_data()

        lines_to_print = 30
        for i, line in enumerate(self.unzipped_content.readlines()):
            print(line.decode('ISO-8859-1').rstrip())
            lines_to_print -= 1
            if lines_to_print <= 0:
                break

    def check_unzipped_data(self):
        if not self.unzipped_data_exists:
            raise ValueError("No unzipped data")

    def extract_forms(self) -> None:
        """We use regex based extraction because we got weird errors when parsing the XML"""
        print("Extracting forms")
        self.check_unzipped_data()
        forms_list = []

        xml_content = self.unzipped_content.getvalue().decode('ISO-8859-1')

        pattern = r'<wn20schema:Word rdf:about="&dn;word-(.*?)" wn20schema:lexicalForm="(.*?)"'
        matches = re.findall(pattern, xml_content, re.DOTALL)

        if not matches:
            raise ValueError("No matches found")

        for match in matches:
            form_id, form = match
            forms_list.append(Form(form=form, id=form_id))

        if not forms_list:
            raise ValueError("Found no forms")

        self.forms = forms_list
        print(f"{len(self.forms)} forms extracted")

    def create_dataframe_and_export_csv(self):
        print("Creating dataframe")
        self.check_unzipped_data()
        # Assuming self.forms contains a list of dictionaries with 'id' and 'form' keys
        forms_list = [{'id': entry.id, 'form': entry.form} for entry in self.forms]

        # Separate forms and IDs
        forms = [entry['form'] for entry in forms_list]
        ids = [entry['id'] for entry in forms_list]

        # Create DataFrame
        df = pd.DataFrame({'id': ids, 'form': forms})
        # Print info about the DataFrame
        print(df.info())
        print(df.sample(5))

        # Export DataFrame to a CSV file
        df.to_csv('forms.csv', index=False)
        self.df = df

    def fetch_danish_lexeme_ids(self):
        # SPARQL query to retrieve Danish lexeme IDs
        sparql_query = """
        SELECT ?lexeme WHERE {
            ?lexeme dct:language wd:Q9035.
        }
        """
        self.setup_wbi()

        # Run the SPARQL query
        query_result = execute_sparql_query(sparql_query)

        danish_lexeme_ids = [result['lexeme']['value'] for result in query_result['results']['bindings']]

        self.lexeme_ids = danish_lexeme_ids
        print(f"Fetched {len(self.lexeme_ids)} danish lexemes from Wikidata")

    def fetch_danish_lexeme_ids_without_dannet_property(self):
        # SPARQL query to retrieve Danish lexeme IDs without the "dannet" property
        sparql_query = """
        SELECT ?lexeme WHERE {
            ?lexeme dct:language wd:Q9035.
            FILTER NOT EXISTS {
                ?lexeme wdt:P6140 ?dannetProperty.
            }
        }
        """
        self.setup_wbi()

        # Run the SPARQL query
        query_result = execute_sparql_query(sparql_query)

        danish_lexeme_ids_without_dannet_property = [result['lexeme']['value'] for result in
                                                     query_result['results']['bindings']]

        self.no_dannet_lexeme_ids = danish_lexeme_ids_without_dannet_property
        print(f"Fetched {len(self.no_dannet_lexeme_ids)} danish lexemes from Wikidata currently missing a DanNet 2.2 ID")

    def find_dannet_ids_for_lexemes_and_upload(self):
        """Match forms and upload matches to Wikidata
        NOTE: we only match on the first lemma"""
        # FIXME also compare lexical category
        # TODO find out where the lexical category is stored in DanNet
        if not hasattr(self, 'no_dannet_lexeme_ids'):
            raise ValueError("No list of lexeme IDs without 'dannet' property")

        if not hasattr(self, 'df'):
            raise ValueError("DataFrame not available")

        print("Finding DanNet IDs for lexemes")
        self.setup_wbi()
        if not self.wbi:
            raise ValueError("wbi not setup correctly")
        for lexeme_id in tqdm(self.no_dannet_lexeme_ids, desc="Processing lexemes"):
            logger.info(f"Working on {lexeme_id}")
            lexeme = self.wbi.lexeme.get(entity_id=lexeme_id.replace('http://www.wikidata.org/entity/', ''))
            lemma = str(lexeme.lemmas.get(language="da"))
            logger.info(f"Got lemma {lemma} for {lexeme.get_entity_url()}")

            match = self.df[self.df['form'] == lemma]

            if not match.empty:
                if len(match) > 1:
                    print("Found multiple matches, skipping")
                else:
                    dannet_id = match.iloc[0]['id']
                    logger.info(f"Found match! DanNet 2.2. ID, see https://wordnet.dk/dannet/data/word-{dannet_id}")
                    input("Press enter to upload if it matches or ctrl-c to exit")
                    claim = ExternalID(prop_nr="P6140", value=dannet_id)
                    lexeme.claims.add(claims=[claim])
                    lexeme.write(summary="Added [[Property:P6140]] using [https://github.com/dpriskorn/LexDanNet LexDanNet]")

    def setup_wbi(self):
        if self.wbi is None:
            from wikibaseintegrator.wbi_config import config as wbi_config

            wbi_config['USER_AGENT'] = config.user_agent

            login = Login(user=config.user_name, password=config.bot_password)
            self.wbi = WikibaseIntegrator(
                login=login
            )
            # exit()


# Usage
url_zip_handler = ZipFileHandler(
    zip_file_path="https://repository.clarin.dk/repository/xmlui/bitstream/handle/20.500.12115/25/DanNet-2.2_owl.zip")
url_zip_handler.start()
