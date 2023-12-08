import logging
import re
from xml import etree

import pandas as pd
import requests
import zipfile
from io import BytesIO

from pandas import DataFrame, Series
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


class PartOfSpeech(BaseModel):
    pos: str
    id: str

class ZipFileHandler(BaseModel):
    zip_file_path: AnyHttpUrl
    zip_content: Any = BytesIO()
    forms: List[Form] = list()
    pos: List[PartOfSpeech] = list()
    forms_xml: Any = None
    pos_xml: Any = None
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
        self.extract_part_of_speech()
        self.create_dataframes_and_export_csv()
        self.check_duplicates()
        # self.fetch_danish_lexeme_ids()
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
        self.forms_xml = BytesIO()
        self.pos_xml = BytesIO()

        with zipfile.ZipFile(self.zip_content, 'r') as zip_ref:
            words_owl_files = [file_name for file_name in zip_ref.namelist() if file_name.endswith('words.rdf')]
            pos_owl_files = [file_name for file_name in zip_ref.namelist() if file_name.endswith('part_of_speech.rdf')]

            if not words_owl_files:
                raise ValueError("No 'words.owl' file found in the zip archive.")
            if not pos_owl_files:
                raise ValueError("No 'part_of_speech.rdf' file found in the zip archive.")

            words_owl_file = words_owl_files[0]
            pos_owl_file = pos_owl_files[0]

            self.forms_xml.write(zip_ref.read(words_owl_file))
            self.pos_xml.write(zip_ref.read(pos_owl_file))

            self.forms_xml.seek(0)
            self.pos_xml.seek(0)

    @property
    def unzipped_data_exists(self) -> bool:
        return bool(self.forms_xml)

    def print_head_unzipped_content(self) -> None:
        print("Printing head of unzipped content")
        self.check_unzipped_data()

        lines_to_print = 30
        for i, line in enumerate(self.forms_xml.readlines()):
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

        xml_content = self.forms_xml.getvalue().decode('ISO-8859-1')

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

    def extract_part_of_speech(self) -> None:
        if self.pos_xml is None:
            raise ValueError("pos_xml has no data.")

        xml_content = self.pos_xml.getvalue().decode('ISO-8859-1')

        pattern = r'<rdf:Description rdf:about="&dn;word-(.*?)"><dn_schema:partOfSpeech>(.*?)</dn_schema:partOfSpeech></rdf:Description>'
        matches = re.findall(pattern, xml_content, re.DOTALL)

        pos_list = []

        for match in matches:
            word_id, pos = match
            pos_list.append(PartOfSpeech(pos=pos.strip(), id=word_id))

        self.pos = pos_list
        # for pos_obj in self.pos:
        #     print(f"Part of Speech: {pos_obj.pos}, ID: {pos_obj.id}")
        # exit()

    @staticmethod
    def map_pos_to_pos_id(pos: str):
        pos_lower = pos.lower()
        if pos_lower == 'noun':
            return 'Q1084'
        elif pos_lower == 'adjective':
            return 'Q34698'
        elif pos_lower == 'verb':
            return 'Q24905'
        else:
            return None  # Handling for other cases if needed

    def create_dataframes_and_export_csv(self):
        print("Creating dataframes")
        self.check_unzipped_data()

        data = {'id': [obj.id for obj in self.pos], 'pos': [obj.pos for obj in self.pos]}
        pos_df = pd.DataFrame(data)

        print(pos_df)
        # Print info about the DataFrame
        print(pos_df.info())
        print(pos_df.sample(5))
        # exit()

        # Assuming self.forms contains a list of dictionaries with 'id' and 'form' keys
        forms_list = [{'id': entry.id, 'form': entry.form} for entry in self.forms]

        # Separate forms and IDs
        forms = [entry['form'] for entry in forms_list]
        ids = [entry['id'] for entry in forms_list]

        # Create DataFrame
        forms_df = pd.DataFrame({'id': ids, 'form': forms})
        # Print info about the DataFrame
        print(forms_df.info())
        print(forms_df.sample(5))

        # Performing an inner join on 'id'
        joined_df = pd.merge(pos_df, forms_df, on='id', how='inner')

        # Create 'pos_id' column based on 'pos' (case insensitive)
        joined_df['pos_id'] = joined_df['pos'].apply(self.map_pos_to_pos_id)

        print(joined_df.info())
        print(joined_df.sample(5))

        # Count occurrences of each value in the 'pos' column
        pos_counts = joined_df['pos'].value_counts()
        print(pos_counts)

        # Export DataFrame to a CSV file
        joined_df.to_csv('words.csv', index=False)
        self.df = joined_df
        # exit()

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

    def check_duplicates(self):
        # Check for duplicates in the 'id' column
        duplicates = self.df['id'].duplicated()

        # Count the number of duplicates
        num_of_duplicates = duplicates.sum()

        if num_of_duplicates == 0:
            print("All IDs are unique!")
        else:
            raise ValueError(f"There are {num_of_duplicates} duplicate IDs in the dataframe.")

    def find_dannet_ids_for_lexemes_and_upload(self):
        """Match forms and upload matches to Wikidata
        For each lexeme missing we try to match against DanNet using both the lemma and lexical category"""
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
            lexical_category = lexeme.lexical_category
            lexical_category_label = self.wbi.item.get(entity_id=lexical_category).labels.get(language="en")
            senses = lexeme.senses.senses
            please_improve_notice = "No Danish gloss for this sense, please help improve"
            glosses = [sense.glosses.get(language="da") or please_improve_notice for sense in senses]
            print(f"Matching on lemma {lemma} with category {lexical_category_label} for {lexeme.get_entity_url()}")
            for gloss in glosses:
                print(f"Gloss: {gloss}")
            matches: DataFrame = self.df[self.df['form'] == lemma]
            if matches.empty:
                print(f"Found no match for lemma '{lemma}' in DanNet")
            else:
                # print(matches)
                # print(type(matches))
                # Iterating through rows using iterrows()
                for index, match in matches.iterrows():
                    category_match = False
                    dannet_pos = match['pos']
                    dannet_id = match['id']
                    dannet_pos_id = match['pos_id']
                    # If a match is found based on lemma, now check for matching lexical category
                    if dannet_pos_id == lexical_category:
                        category_match = True
                    if category_match:
                        # Perform further action as the lexical category matches the pos_id in the DataFrame
                        print(f"Match found! See https://wordnet.dk/dannet/data/word-{dannet_id}")
                        input("Press enter to upload if it matches or ctrl-c to exit")
                        claim = ExternalID(prop_nr="P6140", value=dannet_id)
                        lexeme.claims.add(claims=[claim])
                        lexeme.write(
                            summary="Added [[Property:P6140]] using [[Wikidata:Tools/LexDanNet]]")
                        print("Upload successful")
                    else:
                        # Handle case where the lexical category does not match pos_id in the DataFrame
                        print("Found matching lemma but the lexical categories do not add up:\n"
                              f"Lemma: {lemma}\n"
                              f"DanNet: {dannet_pos}\n"
                              f"Wikidata: {lexical_category_label}")

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
