import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import ClassVar

EMPTY_STR = "EMPTY_STR"


class SelectableFromSQLite:
    sqlite_select_query: ClassVar[str]
    table_name: ClassVar[str]
    id: uuid.UUID

    def __init__(self, *args):
        raise NotImplementedError


@dataclass
class Filmwork(SelectableFromSQLite):
    table_name: ClassVar[str] = "film_work"
    sqlite_select_query: ClassVar[
        str
    ] = """
        SELECT title, description, type, creation_date, certificate,
        file_path, created_at, updated_at, rating, id
        FROM film_work;
        """
    title: str
    description: str
    type: str
    creation_date: datetime
    certificate: str
    file_path: str
    created: datetime
    modified: datetime

    rating: float = field(default=0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self):
        if self.title is None:
            self.title = EMPTY_STR

        if self.type is None:
            self.type = EMPTY_STR


@dataclass
class Person(SelectableFromSQLite):
    table_name: ClassVar[str] = "person"
    sqlite_select_query: ClassVar[
        str
    ] = """
    SELECT full_name, birth_date, created_at, updated_at, id
    FROM person;"""
    full_name: str
    birth_date: date
    created: datetime
    modified: datetime

    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmwork(SelectableFromSQLite):

    table_name: ClassVar[str] = "person_film_work"
    sqlite_select_query: ClassVar[
        str
    ] = """
        SELECT film_work_id, person_id, role, created_at, id
        FROM person_film_work;
        """
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre(SelectableFromSQLite):

    table_name: ClassVar[str] = "genre"
    sqlite_select_query: ClassVar[
        str
    ] = """
    SELECT name, description, created_at, updated_at, id FROM genre;
    """
    name: str
    description: str
    created: datetime
    modified: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmwork(SelectableFromSQLite):

    table_name: ClassVar[str] = "genre_film_work"
    sqlite_select_query: ClassVar[
        str
    ] = """
    SELECT film_work_id, genre_id, created_at, id FROM genre_film_work;
    """
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
