from pydantic import BaseModel


class Configuration(BaseModel):
    name: str
    type: str
    description: str


class Unit(BaseModel):
    name: str
    long_name: str
    aliases: list[str]