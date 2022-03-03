import psycopg2
from psycopg2.extras import DictCursor

from app.sqlite_to_postgres.loaders.settings import get_dsl
from etl.extracters import FilmworkExtracter
from etl.transformers import PgToESTransformer


def main():
    transformer = PgToESTransformer()
    with psycopg2.connect(
        **get_dsl(".env"), cursor_factory=DictCursor
    ) as pg_conn:
        extracter = FilmworkExtracter(pg_connection=pg_conn, batch_size=1)
        extracted = extracter.extract()
    transformed = transformer.transform(extracted)
    print([es_data.json() for es_data in transformed])


if __name__ == "__main__":
    main()
