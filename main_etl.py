import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from sqlite_to_postgres.loaders.settings import get_dsl
from etl.backoff import backoff
from etl.extracters import FilmworkExtracter, GenreExtracter, PersonExtracter
from etl.loaders import Loader
from etl.pipelines import MoviesETL
from etl.transformers import PgToESTransformer

PATH_TO_ENV = ".env"
load_dotenv(PATH_TO_ENV)


@backoff()
def main():
    # create simple items - they do the same for all etls
    transformer = PgToESTransformer()
    loader = Loader(
        index="movies",
        es_url="http://{es_host}:{es_port}".format(
            es_port=os.environ.get("ES_PORT"),
            es_host=os.environ.get("ES_HOST")
        ),
    )
    with psycopg2.connect(
        **get_dsl(),
        cursor_factory=DictCursor,
    ) as pg_conn:
        # vary extractor for genre, fw, person
        for extracter in (
            PersonExtracter(
                pg_connection=pg_conn,
                state_path="states/person_state.json",
                batch_size=50,
            ),
            GenreExtracter(
                pg_connection=pg_conn,
                state_path="states/genre_state.json",
                batch_size=2,
            ),
            FilmworkExtracter(
                pg_connection=pg_conn,
                state_path="states/fw_state.json",
                batch_size=10,
            ),
        ):
            # combine etl
            etl = MoviesETL(
                extracter=extracter,
                transformer=transformer,
                loader=loader,
            )
            # and run it
            etl.run()


if __name__ == "__main__":
    main()
