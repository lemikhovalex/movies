import json
import logging
from typing import Callable, List

from elasticsearch import Elasticsearch, helpers

from etl.pg_to_es.base import ILoader
from etl.pg_to_es.data_structures import ToES
from etl.utils import process_exception

LOGGER_NAME = "loader.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))


class Loader(ILoader):
    def __init__(self, index: str, es_factory: Callable[[], Elasticsearch]):
        self.es_conn_factory = es_factory
        self.index = index

    def load(self, data_to_load: List[ToES]):
        actions = []
        es_conn = self.es_conn_factory()
        for datum in data_to_load:
            to_app = json.loads(datum.json())
            to_app["_id"] = datum.id
            actions.append(to_app)
        # if we dont match index we shall not wait, we better write
        try:
            helpers.bulk(
                es_conn,
                actions,
                index=self.index,
            )
        except helpers.BulkIndexError as excep:
            process_exception(excep, logger)

    def save_state(self):
        pass
