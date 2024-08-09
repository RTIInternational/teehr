"""Pydantic models for domain table entries."""
from pydantic import BaseModel


class Configuration(BaseModel):
    """Configuration model."""

    name: str
    type: str
    description: str


class Unit(BaseModel):
    """Unit model."""

    name: str
    long_name: str


class Variable(BaseModel):
    """Variable model."""

    name: str
    long_name: str


class Attribute(BaseModel):
    """Variable model."""

    name: str
    type: str
    description: str
