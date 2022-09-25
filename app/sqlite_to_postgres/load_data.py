import sqlite3
from typing import List, Type

import psycopg2
from psycopg2.extras import DictCursor

from loaders import get_dsl
from loaders.loaders import PostgresSaver, SQLiteDownLoader
from loaders.tables import (
    Filmwork,
    Genre,
    GenreFilmwork,
    Person,
    PersonFilmwork,
    SelectableFromSQLite,
)

BATCH_SIZE = 256


if __name__ == "__main__":

    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg2.connect(
        **get_dsl("../../.env"),
        cursor_factory=DictCursor,
    ) as pg_conn:

        down_loader = SQLiteDownLoader(connect=sqlite_conn)
        up_loader = PostgresSaver(connect=pg_conn)
        # load and save tables in cycle
        tables: List[Type[SelectableFromSQLite]] = [
            Genre,
            Person,
            Filmwork,
            GenreFilmwork,
            PersonFilmwork,
        ]
        for table in tables:
            table_loader = down_loader.load_class(
                constructor=table,
                batch_size=BATCH_SIZE,
            )
            for batch in table_loader:
                up_loader.insert_to_table(
                    batch,
                    table_name=table.table_name,
                )
        pg_conn.commit()
