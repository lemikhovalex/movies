import datetime
import logging
from abc import ABC, abstractmethod
from typing import Generator, List, Optional, Tuple

from .data_structures import MergedFromPg
from .etl_interfaces import IExtracter
from .state import JsonFileStorage, State

logger = logging.getLogger(__name__)

FMT = "%Y%m%d%H%M%S"  # ex. 20110104172008 -> Jan. 04, 2011 5:20:08pm
INIT_DATE = datetime.datetime(2010, 2, 8, 1, 40, 27, 425337)


def date_time_to_str(date_t: datetime.datetime) -> str:
    return date_t.strftime(FMT)


def str_to_date_time(date_t: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_t, FMT)


def fetch_upd_ids_from_table(
    pg_connection, table: str, batch_size: int, state: State
) -> list:
    with pg_connection.cursor() as cursor:
        query = """
            SELECT id, modified
            FROM content.{tbl}
            WHERE modified > %s
            ORDER BY modified
            OFFSET %s
            LIMIT %s
        """.format(
            tbl=table,
        )
        last_mod = str_to_date_time(state.get_state("last_load"))
        try:
            cursor.execute(
                query,
                (last_mod, state.get_state("prod_offset"), batch_size),
            )
        except Exception as exc_fetch:  # todo error handling
            msg = "Failed to execute following query: {q}".format(
                q=query,
            )
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch
        try:
            fetched_ids = cursor.fetchmany(batch_size)
        except Exception as exc_fetch:  # todo error handling
            msg = "Failed to execute following query: {q}".format(
                q=query,
            )
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch

        fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]
        return fetched_ids


def merge_data_on_fw_ids(
    pg_connection,
    fw_ids: list,
) -> List[MergedFromPg]:
    with pg_connection.cursor() as cursor:
        query = MergedFromPg.select_query
        try:
            cursor.execute(
                query,
                (tuple(fw_ids),),
            )
        except Exception as exc_fetch:  # todo error handling
            msg = "Failed to execute following query: {q}".format(q=query)
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch
        try:
            fetched_data = cursor.fetchall()  # todo fetch all, really?
        except Exception as exc_fetch:  # todo error handling
            msg = "Failed to execute following query: {q}".format(
                q=query,
            )
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch
        # completed fine, have some more, now upd offset
        fetched_data = [MergedFromPg(*args) for args in fetched_data]
        return fetched_data


def enrich(
    pg_connection,
    table: str,
    ids: list,
    batch_size: int,
    state: State,
    field: Optional[str] = None,
    m2m_table: Optional[str] = None,
) -> list:
    with pg_connection.cursor() as cursor:
        if field is None:
            field = "{tbl}_id".format(tbl=table)
        if m2m_table is None:
            m2m_table = "{tbl}_film_work".format(tbl=table)
        query = """
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.{m2m_tbl} m2m_tbl ON m2m_tbl.film_work_id = fw.id
            WHERE m2m_tbl.{fld} IN %s
            ORDER BY fw.modified;
        """.format(
            fld=field,
            m2m_tbl=m2m_table,
        )

        try:
            cursor.execute(
                query,
                (tuple(ids),),
            )
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
            msg = "Failed to fetch from following query: {q}".format(
                q=query,
            )
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch

        # completed fine, have some more, now upd offset
        fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]
        return fetched_ids


class IPEMExtracter(IExtracter, ABC):
    state: State

    @abstractmethod
    def produce(self) -> Tuple[list, bool]:
        pass

    @abstractmethod
    def enrich(self, ids: list) -> list:
        pass

    @abstractmethod
    def merge(self, ids: list) -> list:
        pass

    def extract(self) -> Generator[list, None, None]:
        is_all_produced = False
        while not is_all_produced:
            proxy_ids, is_all_produced = self.produce()
            target_ids = self.enrich(proxy_ids)

            yield self.merge(target_ids)


class FilmworkExtracter(IPEMExtracter):
    table = "film_work"

    def __init__(self, pg_connection, batch_size: int = 1):
        self._connect = pg_connection
        self._last_modified = ""
        self.batch_size = batch_size
        storage = JsonFileStorage(
            "{tbl}_loader_state.json".format(tbl=self.table),
        )
        self.state = State(storage)
        self.state.set_state("prod_offset", 0)
        self.state.set_state(
            "last_load",
            date_time_to_str(INIT_DATE),
        )

    def produce(self) -> Tuple[list, bool]:
        out = fetch_upd_ids_from_table(
            pg_connection=self._connect,
            table=self.table,
            batch_size=self.batch_size,
            state=self.state,
        )
        is_done = False
        if len(out) < self.batch_size:
            is_done = True
            new_offset = 0
            self.state.set_state(
                "last_load",
                date_time_to_str(datetime.datetime.now()),
            )
        # completed fine, have some more, now upd offset
        else:
            offset_before = int(self.state.get_state("prod_offset"))
            new_offset = offset_before + self.batch_size
        self.state.set_state("prod_offset", new_offset)
        return out, is_done

    def enrich(self, ids: list) -> list:
        return ids

    def merge(self, ids: list) -> List[MergedFromPg]:
        if len(ids) == 0:
            return []
        return merge_data_on_fw_ids(pg_connection=self._connect, fw_ids=ids)


class GenreExtracter(IPEMExtracter):
    table = "genre"

    def __init__(self, pg_connection, batch_size: int = 1):
        self._connect = pg_connection
        self._last_modified = ""
        self.batch_size = batch_size
        storage = JsonFileStorage(
            "{tbl}_loader_state.json".format(tbl=self.table),
        )
        self.state = State(storage)
        self.state.set_state("prod_offset", 0)
        self.state.set_state(
            "last_load",
            date_time_to_str(INIT_DATE),
        )

    def produce(self) -> Tuple[list, bool]:
        out = fetch_upd_ids_from_table(
            pg_connection=self._connect,
            table=self.table,
            batch_size=self.batch_size,
            state=self.state,
        )
        is_done = False
        if len(out) < self.batch_size:
            is_done = True
            new_offset = 0
            self.state.set_state(
                "last_load",
                date_time_to_str(datetime.datetime.now()),
            )
        # completed fine, have some more, now upd offset
        else:
            offset_before = int(self.state.get_state("prod_offset"))
            new_offset = offset_before + self.batch_size
        self.state.set_state("prod_offset", new_offset)
        return out, is_done

    def enrich(self, ids: list) -> list:
        if len(ids) == 0:
            return []
        return enrich(
            pg_connection=self._connect,
            table=self.table,
            ids=ids,
            batch_size=self.batch_size,
            state=self.state,
        )

    def merge(self, ids: list) -> List[MergedFromPg]:
        if len(ids) == 0:
            return []
        return merge_data_on_fw_ids(
            pg_connection=self._connect,
            fw_ids=ids,
        )


class PersonExtracter(IPEMExtracter):
    table = "person"

    def __init__(self, pg_connection, batch_size: int = 1):
        super(PersonExtracter, self).__init__(pg_connection, batch_size)
