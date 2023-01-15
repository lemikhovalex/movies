import datetime
from dataclasses import dataclass
from typing import ClassVar, List, Optional

from pydantic import BaseModel, Field

from etl.pg_to_es.base import WithQuery


@dataclass
class MergedFromPg(WithQuery):
    select_query: ClassVar[
        str
    ] = """
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id,
            p.full_name,
            g.id,
            g.name
        FROM content.film_work fw
        LEFT OUTER JOIN content.person_film_work pfw
            ON pfw.film_work_id = fw.id
        LEFT OUTER JOIN content.person p
            ON p.id = pfw.person_id
        LEFT OUTER JOIN content.genre_film_work gfw
            ON gfw.film_work_id = fw.id
        LEFT OUTER JOIN content.genre g
            ON g.id = gfw.genre_id
        WHERE fw.id IN %s;
        """
    film_work_id: str
    title: str
    description: str
    imdb_rating: float
    fw_type: str
    created: datetime.datetime
    modified: datetime.datetime
    role: str
    person_id: str
    person_full_name: str
    genre_id: str
    genre_name: str


@dataclass
class ESPerson:
    id: str
    name: str


@dataclass
class ESGenre:
    id: str
    name: str


class ToES(BaseModel):
    id: str = Field(alias="film_work_id")
    imdb_rating: Optional[float]
    genres: List[ESGenre]
    genres_names: List[str]
    title: str
    description: Optional[str]
    directors: List[ESPerson]
    directors_names: List[str]

    writers_names: List[str]
    writers: List[ESPerson]

    actors: List[ESPerson]
    actors_names: List[str]
