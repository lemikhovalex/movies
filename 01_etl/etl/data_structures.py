import datetime
from dataclasses import dataclass
from typing import ClassVar

from .etl_interfaces import WithQuery


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
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw
            ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p
            ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw
            ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g
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
    genre_name: str
