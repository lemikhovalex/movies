from functools import lru_cache
from typing import Type

from db.base import BaseStorage
from db.elastic import get_elastic
from fastapi import Depends
from models.genre import Genre
from services.paginators import ESQueryPaginator

from .base import BaseService


class GenreService(BaseService):
    def _index_name(self) -> str:
        return "genres"

    def _result_class(self) -> Type[Genre]:
        return Genre


@lru_cache()
def get_genre_service(
    elastic: BaseStorage = Depends(get_elastic),
) -> GenreService:
    return GenreService(storage=elastic, paginator=ESQueryPaginator)
