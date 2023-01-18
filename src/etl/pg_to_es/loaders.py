import json
import logging
import os
from typing import Callable, List

from elasticsearch import Elasticsearch, helpers

from etl.config import CONFIG
from etl.pg_to_es.base import ILoader
from etl.pg_to_es.data_structures import ToES
from etl.utils import process_exception

if CONFIG.logger_path is not None:
    LOGGER_NAME = os.path.join(CONFIG.logger_path, "loader.log")
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(logging.FileHandler(LOGGER_NAME))
else:
    logger = logging


class Loader(ILoader):
    def __init__(
        self, index: str, es_factory: Callable[[], Elasticsearch], debug: bool = False
    ):
        self.es_conn_factory = es_factory
        self.index = index
        self.debug = debug

    def load(self, data_to_load: List[ToES]):
        actions = []

        with self.es_conn_factory() as es_conn:
            for datum in data_to_load:
                to_app = json.loads(datum.json())
                to_app["_id"] = datum.id
                actions.append(to_app)
            # if we dont match index we shall not wait, we better write
            try:
                helpers.bulk(es_conn, actions, index=self.index, refresh=self.debug)
            except helpers.BulkIndexError as excep:
                process_exception(excep, logger)

    def save_state(self):
        pass
