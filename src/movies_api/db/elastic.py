import logging
from typing import Optional

from db.base import BaseStorage, QueryParam, SortParam
from elasticsearch import AsyncElasticsearch, exceptions

logger = logging.getLogger(__name__)


class ESStorage(BaseStorage):
    def __init__(self, elastic: AsyncElasticsearch):
        self.es = elastic

    async def get_by_id(self, index: str, id):
        try:
            doc = await self.es.get(
                index=index,
                id=str(id),
            )
        except exceptions.NotFoundError:
            return None
        return doc

    async def get_with_search(
        self,
        query: QueryParam,
        sort: SortParam,
        search_after=None,
        size: Optional[int] = None,
        from_: Optional[int] = None,
        pit: Optional[str] = None,
    ):
        logger.debug("sort params: \n\n {s}\n\n".format(s=sort.dict(by_alias=True)))
        logger.debug("query params: \n\n {s}\n\n".format(s=query.dict(by_alias=True)))
        return await self.es.search(
            query=query.dict(by_alias=True),
            sort=sort.dict(by_alias=True)["fields"],
            pit={"id": pit, "keep_alive": "1m"},
            size=size,
            from_=from_,
            search_after=search_after,
        )

    async def open_pit(self, index: str, keep_alive="1m"):
        return await self.es.open_point_in_time(index=index, keep_alive=keep_alive)

    async def close_pit(self, id: str):
        return await self.es.close_point_in_time(id=id)

    async def close(self):
        return await self.es.close()


es: ESStorage


async def get_elastic() -> ESStorage:
    return es  # noqa: F821
