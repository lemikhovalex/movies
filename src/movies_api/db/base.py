from abc import ABC, abstractmethod

from pydantic import BaseModel, Field


class QueryOptions(BaseModel):
    must: list
    should: list


class QueryParam(BaseModel):
    bool_: QueryOptions = Field(QueryOptions(must=[], should=[]), alias="bool")


class SortFieldOption(BaseModel):
    order: str = "desc"


class SortParam(BaseModel):
    fields: list[dict[str, SortFieldOption]]


class BaseStorage(ABC):
    @abstractmethod
    async def get_by_id(self, index: str, id) -> dict:
        pass

    @abstractmethod
    async def get_with_search(
        self, query: QueryParam, sort: SortParam, freeze_idx: dict, **kwargs
    ) -> dict:
        pass

    @abstractmethod
    async def close():
        pass
