from uuid import UUID

from pydantic import BaseModel


class BasePerson(BaseModel):
    id: UUID
    name: str


class Person(BasePerson):
    roles: list[str]


class Genre(BaseModel):
    id: UUID
    name: str


class FilmWork(BaseModel):
    id: UUID
    imdb_rating: float
    title: str
    description: str
    directors_names: list[str]
    actors_names: list[str]
    writers_names: list[str]
    genres: list[Genre]
    actors: list[BasePerson]
    directors: list[BasePerson]
    writers: list[BasePerson]
