import logging
from typing import List, Hashable

import inquirer
from pandas import DataFrame, Series
from pydantic import BaseModel
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.datatypes import ExternalID, Item
from wikibaseintegrator.entities import LexemeEntity
from wikibaseintegrator.models import LanguageValue

logger = logging.getLogger(__name__)


class Lexeme(BaseModel):
    """This model handles all the matching on the lexeme level"""

    lexeme_id: str
    df: DataFrame
    wbi: WikibaseIntegrator
    already_matched_and_uploaded: bool = False
    lexeme: LexemeEntity = None
    lemma: str = ""
    lexical_category_qid: str = ""
    lexical_category_label: str = ""
    matches: DataFrame = None
    skip: bool = False

    class Config:
        arbitrary_types_allowed = True

    def match(self):
        self.prepare()
        self.do_matching()

    def prepare(self):
        logger.info(f"Working on {self.lexeme_id}")
        self.lexeme = self.wbi.lexeme.get(entity_id=self.get_stripped_qid)
        self.lemma = str(self.lexeme.lemmas.get(language="da"))
        self.lexical_category_qid = self.lexeme.lexical_category
        self.lexical_category_label = self.wbi.item.get(
            entity_id=self.lexical_category_qid
        ).labels.get(language="en")

    def print_details(self):
        print(
            f"Matching on lemma '{self.lemma}' with category "
            f"{self.lexical_category_label} for {self.lexeme.get_entity_url()}"
        )
        for gloss in self.glosses:
            print(f"Gloss: {gloss}")

    def do_matching(self):
        self.matches: DataFrame = self.df[self.df["form"] == self.lemma]
        if self.matches.empty:
            self.upload_missing_in_statement()
        # else:
        #     self.print_details()
        #     self.iterate_matches()
        #     print("---")

    def upload_missing_in_statement(self):
        logger.info(
            f"Found no match for lemma '{self.lemma}' "
            f"with lexical category {self.lexical_category_label} in DanNet"
        )
        print("Lemma missing in DanNet. Uploading missing in -> DanNet 2.2 statement")
        # input("press enter to continue")
        claim = Item(prop_nr="P9660", value="Q123739672")
        self.lexeme.claims.add(claims=[claim])
        self.lexeme.write(
            summary="Added [[Property:P9660]]->[[Q123739672]] using [[Wikidata:Tools/LexDanNet]]"
        )
        print("Upload successful")

    def iterate_matches(self):
        # print(matches)
        # print(type(matches))
        # Make sure we have Danish glosses for all senses
        self.handle_no_senses()
        if not self.skip:
            self.make_sure_we_have_danish_glosses_for_all_senses()
            self.check_that_we_have_all_glosses_we_need()
            # Iterating through rows using iterrows()
            for index, match in self.matches.iterrows():
                self.match_row(index=index, match=match)

    def match_row(self, index: Hashable, match: Series):
        if self.already_matched_and_uploaded:
            print(
                "Skipping checking more matches with this "
                "lexeme because we already uploaded a DanNet id"
            )
        else:
            logger.info(f"Iterating match {index}/{len(self.matches)}")
            category_match = False
            dannet_pos = match["pos"]
            dannet_id = match["id"]
            dannet_pos_qid = match["pos_id"]
            logger.info(
                f"id: {dannet_id}, pos: {dannet_pos}, pos_qid: {dannet_pos_qid}"
            )
            # If a match is found based on lemma, now check for matching lexical category
            if dannet_pos_qid == str(self.lexical_category_qid):
                category_match = True
                print("Found category match")
            if category_match:
                # Perform further action as the lexical category matches the pos_id in the DataFrame
                print(
                    f"Match found! See https://wordnet.dk/dannet/data/word-{dannet_id}"
                )
                if self.match_approved():
                    self.upload_match(dannet_id=dannet_id)
                else:
                    print("Match rejected")
            else:
                # Handle case where the lexical category does not match pos_id in the DataFrame
                logger.info(
                    "Found matching lemma but the lexical categories do not add up:\n"
                    f"Lemma: {self.lemma}\n"
                    f"DanNet: {dannet_pos}\n"
                    f"Wikidata: {self.lexical_category_label}"
                )

    def upload_match(self, dannet_id: str):
        print("Match was approved, uploading...")
        # input("Press enter to upload if it matches or ctrl-c to exit")
        claim = ExternalID(prop_nr="P6140", value=dannet_id)
        self.lexeme.claims.add(claims=[claim])
        self.lexeme.write(
            summary="Added [[Property:P6140]] using [[Wikidata:Tools/LexDanNet]]"
        )
        print("Upload successful")
        self.already_matched_and_uploaded = True

    @property
    def get_stripped_qid(self) -> str:
        return self.lexeme_id.replace("http://www.wikidata.org/entity/", "")

    @staticmethod
    def ask_user_to_add_glosses():
        print(
            f"Please add Danish glosses on all senses on this "
            f"lexeme to procede to matching."
        )
        # f"We recommend using the new copy sense script available here: "
        # f"https://www.wikidata.org/wiki/User:Jon_Harald_S%C3%B8by/copySenses.js")
        input("Press enter to continue")

    @property
    def senses(self):
        return self.lexeme.senses.senses

    @property
    def glosses(self) -> List[LanguageValue]:
        return [
            sense.glosses.get(language="da")
            for sense in self.senses
            if sense.glosses.get(language="da") is not None
        ]

    def make_sure_we_have_danish_glosses_for_all_senses(self):
        if self.senses and not self.glosses or len(self.senses) != len(self.glosses):
            print(
                f"We are missing a Danish gloss on "
                f"{len(self.senses) - len(self.glosses)} sense(s)"
            )
            while len(self.senses) != len(self.glosses):
                self.ask_user_to_add_glosses()
                # Reload lexeme with hopefully new glosses added
                self.lexeme = self.wbi.lexeme.get(entity_id=self.get_stripped_qid)

    def handle_no_senses(self):
        if not self.senses:
            # TODO support importing senses from DanNet/DDO and
            #  use a GPT like chatgpt to rephrase/wash the gloss to avoid copyright
            print(
                "No sense in Wikidata which we don't support. "
                f"Please add at least one sense to match on "
                f"{self.lexeme.get_entity_url()}. Skipping"
            )
            self.skip = True

    def check_that_we_have_all_glosses_we_need(self):
        if self.senses and len(self.senses) == len(self.glosses):
            print(f"Hooray, we have Danish glosses for all {len(self.senses)} senses!")

    @staticmethod
    def match_approved() -> bool:
        """Ask user if match is good"""
        question = [
            inquirer.List(
                "choice",
                message="Is this a good match?",
                choices=["Yes", "No"],
                default="No",
            ),
        ]
        answer = inquirer.prompt(question)
        if answer["choice"] == "Yes":
            return True
        else:
            return False
