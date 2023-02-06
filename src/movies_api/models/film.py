from typing import Optional

from .base import BaseModel
from .genre import BaseGenre
from .person import BasePerson


class Film(BaseModel):
    imdb_rating: Optional[float]
    genres: list[BaseGenre]
    title: str
    description: Optional[str]
    actors: list[BasePerson]
    writers: list[BasePerson]
    directors: list[BasePerson]
