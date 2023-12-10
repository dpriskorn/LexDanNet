from pydantic import BaseModel


class PartOfSpeech(BaseModel):
    pos: str
    id: str
