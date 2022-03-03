import json
import logging
from typing import List

from elasticsearch import Elasticsearch, helpers

from .data_structures import ToES
from .etl_interfaces import ILoader
from .utils import process_exception

logger = logging.getLogger("loader.log")


class Loader(ILoader):
    def __init__(self, index: str):
        self.index = index
        pass

    def load(self, data_to_load: List[ToES]):
        el_s_client = Elasticsearch("http://127.0.0.1:9200")
        actions = []
        for datum in data_to_load:
            to_app = json.loads(datum.json())
            to_app["_id"] = datum.id
            actions.append(to_app)
        # if we dont match index we shall not wait, we better write
        try:
            helpers.bulk(
                el_s_client,
                actions,
                index=self.index,
            )
        except helpers.BulkIndexError as excep:
            process_exception(excep, logger)

    def save_state(self):
        pass
