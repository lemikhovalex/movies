from typing import Callable

from elasticsearch import Elasticsearch

from etl.pg_to_es.extracters import FilmworkExtracter, GenreExtracter, PersonExtracter
from etl.pg_to_es.loaders import Loader
from etl.pg_to_es.pipelines import MoviesETL
from etl.pg_to_es.transformers import PgToESTransformer


def main(pg_conn, es_factory: Callable[[], Elasticsearch]):
    # create simple items - they do the same for all etls
    transformer = PgToESTransformer()
    loader = Loader(
        index="movies",
        es_factory=es_factory,
    )

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
