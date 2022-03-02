import psycopg2
from psycopg2.extras import DictCursor

from app.sqlite_to_postgres.loaders.settings import get_dsl
from etl.extracters import FilmworkExtracter


def main():
    with psycopg2.connect(
        **get_dsl(".env"), cursor_factory=DictCursor
    ) as pg_conn:
        extracter = FilmworkExtracter(pg_connection=pg_conn, batch_size=5)
        extracted, is_all = extracter.extract()
        g, _ = extracter.extract()
    print("extractor returns", type(extracted))
    print("first list is len", len(extracted))
    print("each element of this list is", type(extracted[0]))
    print("and here is example", extracted[0])
    print("\n\nand here is example", g[0])


if __name__ == "__main__":
    main()
