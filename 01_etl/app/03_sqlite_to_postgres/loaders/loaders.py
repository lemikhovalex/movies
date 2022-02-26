import logging
from dataclasses import asdict
from typing import Generator, List, Type

from psycopg2.extras import execute_values

from .tables import SelectableFromSQLite

logging.basicConfig(
    filename=".log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def get_pattern_for_morgify(number_of_keys: int) -> str:
    """Generate pattern for inserting batch with morgify.

    Args:
        number_of_keys (int): number of keys to be inserted

    Returns:
        str: '(%s,%s,...,%s)' where number of %s mathces arg number_of_keys
    """
    joindes_s = ",".join(["%s" for _ in range(number_of_keys)])
    return "({_jnd_s})".format(_jnd_s=joindes_s)


class PostgresSaver(object):
    """Loads data to postgres."""

    def __init__(self, connect):
        """Initialize with connection to db.

        Args:
            connect ([type]): connection to bd. furthe cursor will be created
        """
        self._connect = connect

    def insert_to_table(
        self,
        table_batch: List[SelectableFromSQLite],
        table_name: str,
    ) -> None:
        """Insert list of objects to table.

        Args:
            table_batch (List[SelectableFromSQLite]):
                data to be inserted to table
            table_name (str): name of acceptor table
        """
        if not table_batch:
            return  # have nothing to insert

        with self._connect.cursor() as cursor:
            keys = asdict(table_batch[0]).keys()

            query = """
                INSERT INTO content.{table_name} ({_keys}) VALUES %s;
            """.format(
                table_name=table_name,
                _keys=", ".join(keys),
            )
            try:
                execute_values(
                    cursor,
                    query,
                    [tuple(asdict(t_e).values()) for t_e in table_batch],
                )
            except Exception as exc_insert:
                msg = "Exception raised on query {q}".format(q=query)
                cursor.close()
                logger.info(msg)
                logger.exception(str(exc_insert))
                raise ValueError from exc_insert
            if "modified" in keys:
                ids = ["'{uiid}'".format(uiid=el.id) for el in table_batch]
                query = """
                UPDATE content.{table} SET modified=NOW() WHERE id in ({args})
                """.format(
                    table=table_name,
                    args=", ".join(ids),
                )
                try:
                    cursor.execute(query)
                except Exception as exc_modify:
                    msg = "Exception raised on update query {q}".format(
                        q=query,
                    )
                    cursor.close()
                    logger.info(msg)
                    logger.exception(str(exc_modify))
                    raise ValueError(msg) from exc_modify


class SQLiteDownLoader(object):
    """Fetch data from SQLite."""

    def __init__(self, connect):
        """Create class for fetching data from SQLite with connection.

        Args:
            connect ([type]): connection to db
        """
        self._connect = connect

    def load_class(
        self,
        constructor: Type[SelectableFromSQLite],
        batch_size: int,
    ) -> Generator[List[SelectableFromSQLite], None, None]:
        """Return generator of batches.

        Args:
            constructor (Type[SelectableFromSQLite]): constructor for data
                from SQLite. also contains table name and SELECT query

            batch_size (int): size of batch to be fetched

        Raises:
            exc_construct: failed to cunstruc data with provided 'constructor'

            exc_fetch: failed to fetch data from table and query
                provided with constructor

        Yields:
            Generator[List[SelectableFromSQLite], None, None]: [description]

        """
        cursor = self._connect.cursor()
        try:
            cursor.execute(constructor.sqlite_select_query)
        except Exception as exc_fetch:
            msg = "Failed to execute following query: {q}".format(
                q=constructor.sqlite_select_query,
            )
            cursor.close()
            logger.info(msg)
            logger.exception(str(exc_fetch))
            raise ValueError(msg) from exc_fetch

        terminate = False
        while not terminate:
            fetched = cursor.fetchmany(batch_size)
            # check if this is the end of the query result
            if len(fetched) < batch_size:
                terminate = True
            # parse data to output type
            # todo save parse
            try:
                out = [constructor(*class_args) for class_args in fetched]
            except Exception as exc_construct:
                msg = "".join(
                    [
                        "Failed to build data for table {t} ".format(
                            t=constructor.table_name,
                        ),
                        "with args {a}".format(a=fetched),
                        "\nSelect query following: {q}".format(
                            q=constructor.sqlite_select_query,
                        ),
                    ],
                )
                logger.info(msg)
                logger.exception(str(exc_construct))
                cursor.close()
                raise ValueError(msg) from exc_construct

            yield out
        cursor.close()
