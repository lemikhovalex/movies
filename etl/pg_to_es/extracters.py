import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Optional, Sequence

from psycopg2.errors import SyntaxError

from etl.backoff import backoff
from etl.pg_to_es.base import IExtracter
from etl.pg_to_es.data_structures import MergedFromPg
from etl.state import BaseUniqueStorage, State
from etl.utils import process_exception

LOGGER_NAME = "logs/extracter.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))

FMT = "%Y%m%d%H%M%S"  # ex. 20110104172008 -> Jan. 04, 2011 5:20:08pm
# after all writing datetime.datetime.min to str and back is challanging
INIT_DATE = datetime.datetime(1700, 2, 8, 1, 40, 27, 425337)


def date_time_to_str(date_t: datetime.datetime) -> str:
    return date_t.strftime(FMT)


def str_to_date_time(date_t: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_t, FMT)


@backoff()
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
        # mind syntax
        try:
            cursor.execute(
                query,
                (last_mod, state.get_state("prod_offset"), batch_size),
            )
        except SyntaxError as excep:
            process_exception(excep, logger)

        fetched_ids = cursor.fetchmany(batch_size)
        fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]

        return fetched_ids


@backoff()
def merge_data_on_fw_ids(
    pg_connection,
    fw_ids: Iterable[Any],
) -> List[MergedFromPg]:
    with pg_connection.cursor() as cursor:
        query = MergedFromPg.select_query
        # mind syntax
        try:
            cursor.execute(
                query,
                (tuple(fw_ids),),
            )
        except SyntaxError as excep:
            process_exception(excep, logger)

        # TODO try smth
        fetched_data = cursor.fetchall()
        # completed fine, have some more, now upd offset
        fetched_data = [MergedFromPg(*args) for args in fetched_data]
        return fetched_data


@backoff()
def enrich(
    pg_connection,
    table: str,
    ids: list,
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
            LEFT OUTER JOIN content.{m2m_tbl} m2m_tbl ON m2m_tbl.film_work_id = fw.id
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
        except SyntaxError as excep:
            process_exception(excep, logger)

        fetched_ids = cursor.fetchall()

        # completed fine, have some more, now upd offset
        fetched_ids = [fetched_el[0] for fetched_el in fetched_ids]
        return fetched_ids


class BaseIdsExtracter(IExtracter, ABC):
    @abstractmethod
    def produce_base(self) -> Iterable[Any]:
        pass

    @abstractmethod
    def get_target_ids(self, ids: Sequence[Any]) -> list:
        pass

    def extract(self) -> Iterable[List[Any]]:
        for proxy_ids in self.produce_base():
            yield self.get_target_ids(proxy_ids)

    @abstractmethod
    def save_state(self):
        pass


class IPEMExtracter(BaseIdsExtracter, ABC):
    state: State

    def __init__(self):
        self.state_to_upd = {}

    @abstractmethod
    def merge(self, ids: list) -> list:
        pass

    def extract(self) -> Iterable[List[Any]]:
        targed_ids_ = super().extract()
        for target_ids in targed_ids_:
            yield self.merge(target_ids)

    @abstractmethod
    def save_state(self):
        pass


class GenreExtracter(IPEMExtracter):
    table = "genre"

    def __init__(self, pg_connection, state: State, batch_size: int = 1):
        super(GenreExtracter, self).__init__()
        self._connect = pg_connection
        self._last_modified = ""
        self.batch_size = batch_size

        self.state = state
        if self.state.get_state("prod_offset") is None:
            self.state.set_state("prod_offset", 0)

        if self.state.get_state("last_load") is None:
            self.state.set_state(
                "last_load",
                date_time_to_str(INIT_DATE),
            )

    def produce_base(self) -> Iterable[List[Any]]:
        is_done = False
        while not is_done:
            out = fetch_upd_ids_from_table(
                pg_connection=self._connect,
                table=self.table,
                batch_size=self.batch_size,
                state=self.state,
            )
            if len(out) < self.batch_size:
                is_done = True
                new_offset = 0
                self.state_to_upd["last_load"] = date_time_to_str(
                    datetime.datetime.now(),
                )
            # completed fine, have some more, now upd offset
            else:
                offset_before = int(self.state.get_state("prod_offset"))
                new_offset = offset_before + self.batch_size
            self.state_to_upd["prod_offset"] = new_offset
            self.save_state()
            yield out

    def get_target_ids(self, ids: list) -> list:
        if len(ids) == 0:
            return []
        return enrich(
            pg_connection=self._connect,
            table=self.table,
            ids=ids,
        )

    def merge(self, ids: list) -> List[MergedFromPg]:
        if len(ids) == 0:
            return []
        out = merge_data_on_fw_ids(
            pg_connection=self._connect,
            fw_ids=ids,
        )
        return out

    def save_state(self):
        for key, val in self.state_to_upd.items():
            self.state.set_state(key, val)
        self.state_to_upd = {}


class FilmworkExtracter(GenreExtracter):
    table = "film_work"

    def __init__(self, pg_connection, state: State, batch_size: int = 1):
        super(FilmworkExtracter, self).__init__(pg_connection, state, batch_size)

    def get_target_ids(self, ids: list) -> list:
        return ids


class PersonExtracter(GenreExtracter):
    table = "person"

    def __init__(self, pg_connection, state: State, batch_size: int = 1):
        super(PersonExtracter, self).__init__(pg_connection, state, batch_size)


class TargetExtracer(IExtracter):
    def __init__(
        self, pg_connection, u_storage: BaseUniqueStorage, batch_size: int
    ) -> None:
        self.u_storage = u_storage
        self.batch_size = batch_size
        self.pg_connection = pg_connection

    def extract(self) -> Iterable[List[Any]]:
        logger.info(
            f"Have {len(self.u_storage)} items in q, gonna pop {self.batch_size}."
        )
        ids_batch_iter = self.u_storage.get_iterator(self.batch_size)
        for batch in ids_batch_iter:
            yield merge_data_on_fw_ids(
                fw_ids=batch,
                pg_connection=self.pg_connection,
            )

    def save_state(self):
        ...
