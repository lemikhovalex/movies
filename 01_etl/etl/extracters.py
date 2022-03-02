import datetime
import logging
from typing import List, Tuple

from etl_interfaces import IPEMLoader
from state import JsonFileStorage, State

logger = logging.getLogger(__name__)

FMT = "%Y%m%d%H%M%S"  # ex. 20110104172008 -> Jan. 04, 2011 5:20:08pm


def date_time_to_str(date_t: datetime.datetime) -> str:
    return date_t.strftime(FMT)


def str_to_date_time(date_t: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_t, FMT)


class FilmworkExtracter(IPEMLoader):
    def __init__(self, pg_connection, batch_size: int = 100):
        self._connect = pg_connection
        self._last_modified = ""
        self.batch_size = batch_size
        storage = JsonFileStorage("filmwork_loader_state.json")
        self.state = State(storage)
        self.state.set_state("offset", 0)
        self.state.set_state(
            "last_load",
            date_time_to_str(datetime.datetime(2010, 2, 8, 1, 40, 27, 425337)),
        )

    def produce(self) -> Tuple[list, bool]:
        with self._connect.cursor() as cursor:
            query = """
                SELECT id, modified
                FROM content.film_work
                WHERE modified > %s
                ORDER BY modified
                OFFSET %s
                LIMIT %s
            """
            last_mod = str_to_date_time(self.state.get_state("last_load"))
            cursor.mogrify(
                query,
                (last_mod, self.state.get_state("offset"), self.batch_size),
            )
            try:
                cursor.execute(query)
            except Exception as exc_fetch:  # todo error handling
                msg = "Failed to execute following query: {q}".format(
                    q=query,
                )
                logger.info(msg)
                logger.exception(str(exc_fetch))
                raise ValueError(msg) from exc_fetch
            try:
                fetched_ids = cursor.fetchmany(self.batch_size)
            except Exception as exc_fetch:  # todo error handling
                msg = "Failed to execute following query: {q}".format(
                    q=query,
                )
                logger.info(msg)
                logger.exception(str(exc_fetch))
                raise ValueError(msg) from exc_fetch
            is_done = False
            if len(fetched_ids) < self.batch_size:
                self.state.set_state("offset", 0)
                self.state.set_state(
                    "last_load",
                    date_time_to_str(datetime.datetime.now()),
                )
                is_done = True
            # completed fine, have some more, now upd offset
            offset_before = self.state.get_state("offset")
            self.state.set_state("offset", offset_before + self.batch_size)
            fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]
            return (fetched_ids, is_done)

    def enrich(self, ids: list) -> Tuple[list, bool]:
        return (ids, True)

    def merge(self, ids: list) -> Tuple[List[tuple], bool]:
        with self._connect.cursor() as cursor:
            query = """
                SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created,
                    fw.modified,
                    pfw.role,
                    p.id,
                    p.full_name,
                    g.name
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw
                    ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p
                    ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw
                    ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g
                    ON g.id = gfw.genre_id
                WHERE fw.id IN (%s);
            """
            cursor.mogrify(
                query,
                (ids),
            )
            try:
                cursor.execute(query)
            except Exception as exc_fetch:  # todo error handling
                msg = "Failed to execute following query: {q}".format(
                    q=query,
                )
                logger.info(msg)
                logger.exception(str(exc_fetch))
                raise ValueError(msg) from exc_fetch
            try:
                fetched_ids = cursor.fetchall()  # todo fetch all, really?
            except Exception as exc_fetch:  # todo error handling
                msg = "Failed to execute following query: {q}".format(
                    q=query,
                )
                logger.info(msg)
                logger.exception(str(exc_fetch))
                raise ValueError(msg) from exc_fetch
            is_done = False
            if True:  #  len(fetched_ids) < self.batch_size:
                self.state.set_state("offset", 0)
                self.state.set_state(
                    "last_load",
                    date_time_to_str(datetime.datetime.now()),
                )
                is_done = True
            # completed fine, have some more, now upd offset
            offset_before = self.state.get_state("offset")
            self.state.set_state("offset", offset_before + self.batch_size)
            fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]
            return (fetched_ids, is_done)
