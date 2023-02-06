from functools import lru_cache
from typing import Type
from uuid import UUID

from db.base import BaseStorage, QueryParam
from db.elastic import get_elastic
from fastapi import Depends
from models.person import Person
from services.base import BaseService
from services.paginators import ESQueryPaginator


class PersonService(BaseService):
    def _index_name(self) -> str:
        return "persons"

    def _result_class(self) -> Type[Person]:
        return Person

    def _query_by_name_part(self, value: UUID, query: QueryParam) -> QueryParam:
        # TODO look for escape function or take it from php es client

        query.bool_.must.append({"match": {"name": {"query": value}}})
        return query


@lru_cache()
def get_person_service(
    elastic: BaseStorage = Depends(get_elastic),
) -> PersonService:
    return PersonService(storage=elastic, paginator=ESQueryPaginator)
