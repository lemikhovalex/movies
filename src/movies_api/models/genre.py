from .base import BaseModel


class BaseGenre(BaseModel):
    name: str


class Genre(BaseGenre):
    pass
