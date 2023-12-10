from pydantic import BaseModel


class Form(BaseModel):
    form: str
    id: str
