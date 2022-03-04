import psycopg2
from psycopg2.extras import DictCursor

from app.sqlite_to_postgres.loaders.settings import get_dsl
from etl.backoff import backoff
from etl.extracters import FilmworkExtracter, GenreExtracter, PersonExtracter
from etl.loaders import Loader
from etl.pipelines import MoviesETL
from etl.transformers import PgToESTransformer


@backoff()
def main():
    # create simple items - they do the same for all etls
    transformer = PgToESTransformer()
    loader = Loader(index="movies")
    with psycopg2.connect(
        **get_dsl(".env"),
        cursor_factory=DictCursor,
    ) as pg_conn:
        # vary extractor for genre, fw, person
        for _i, extracter in enumerate(
            (
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
            ),
        ):
            # combine etl
            etl = MoviesETL(
                extracter=extracter,
                loader=loader,
                transformer=transformer,
            )
            # and run it
            etl.run()
            print(_i)


if __name__ == "__main__":
    main()
