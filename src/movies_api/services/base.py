from abc import ABC, abstractmethod
from typing import Optional, Type
from uuid import UUID

from db.base import BaseStorage, QueryParam, SortFieldOption, SortParam
from models.base import BaseModel
from services.paginators import BasePaginator

CACHE_EXPIRE_IN_SECONDS = 1


class BaseService(ABC):
    def __init__(self, storage: BaseStorage, paginator: Type[BasePaginator]):
        self.storage = storage
        self.paginator = paginator

    async def get_by(
        self, page_number: int, page_size: int, sort: Optional[str] = None, **kwargs
    ):

        _sort = SortParam(fields=[{"_score": SortFieldOption(order="desc")}])
        order = "desc"
        if sort is not None:
            if sort.startswith("-"):
                order = "asc"
                sort = sort.removeprefix("-")
            _sort.fields.insert(0, {sort: SortFieldOption(order=order)})
        _query = QueryParam()
        if "id" not in [list(s.keys())[0] for s in _sort.fields]:
            _sort.fields.append({"id": SortFieldOption(order="asc")})
        for method, value in kwargs.items():
            method_name = "_query_by_{m}".format(m=method)
            _query = getattr(self, method_name)(value=value, query=_query)

        paginator = self.paginator(
            query=_query,
            sort=_sort,
            storage=self.storage,
            index=self._index_name(),
            page_size=page_size,
        )
        resp = await paginator.get_page(page_number=page_number)
        results_src = [datum["_source"] for datum in resp["hits"]["hits"]]
        return [self._result_class().parse_obj(src) for src in results_src]

    async def get_by_id(self, entity_id: UUID) -> Optional[BaseModel]:

        doc = await self.storage.get_by_id(
            index=self._index_name(),
            id=str(entity_id),
        )
        if doc is None:
            return None
        else:
            return self._result_class().parse_obj(doc["_source"])

    @abstractmethod
    def _index_name(self) -> str:
        pass

    @abstractmethod
    def _result_class(self) -> Type[BaseModel]:
        pass
