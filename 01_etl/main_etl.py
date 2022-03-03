import json

import psycopg2
from psycopg2.extras import DictCursor

from app.sqlite_to_postgres.loaders.settings import get_dsl
from etl.extracters import FilmworkExtracter, GenreExtracter, PersonExtracter
from etl.loaders import Loader
from etl.transformers import PgToESTransformer


def main():
    transformer = PgToESTransformer()
    with psycopg2.connect(
        **get_dsl(".env"), cursor_factory=DictCursor
    ) as pg_conn:
        for _i, ex_constr in enumerate(
            (FilmworkExtracter, GenreExtracter, PersonExtracter)
        ):
            extracter = ex_constr(pg_connection=pg_conn)
            for extracted in extracter.extract():
                transformed = transformer.transform(extracted)
                loader = Loader(index="movies")
                loader.load(transformed)
            print(_i)


if __name__ == "__main__":
    main()
