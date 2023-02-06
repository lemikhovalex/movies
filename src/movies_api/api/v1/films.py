from http import HTTPStatus
from typing import Optional
from uuid import UUID

from api.v1 import FilmFullInfo, PartialFilmInfo, get_page_params
from api.v1.messages import FILM_NOT_FOUND
from core.config import REDIS_CACHE_EXPIRE
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi_cache.decorator import cache
from services.films import FilmService, get_film_service

router = APIRouter()


@router.get("/search", response_model=list[PartialFilmInfo])
@cache(expire=REDIS_CACHE_EXPIRE)
async def film_search(
    request: Request,
    query: Optional[str] = None,
    page: dict = Depends(get_page_params),
    film_service: FilmService = Depends(get_film_service),
    filter_genre: Optional[UUID] = Query(None, alias="filter[genre]"),
) -> list[PartialFilmInfo]:
    out = await film_service.get_by(
        page_number=page["number"],
        page_size=page["size"],
        query=query,
        genre_id=filter_genre,
    )
    return [PartialFilmInfo(**film.dict()) for film in out]


@router.get("", response_model=list[PartialFilmInfo])
@cache(expire=REDIS_CACHE_EXPIRE)
async def film_search_general(
    request: Request,
    sort: Optional[str] = "imdb_rating",
    page: dict = Depends(get_page_params),
    film_service: FilmService = Depends(get_film_service),
    filter_genre: Optional[UUID] = Query(None, alias="filter[genre]"),
) -> list[PartialFilmInfo]:
    out = await film_service.get_by(
        page_number=page["number"],
        page_size=page["size"],
        sort=sort,
        genre_id=filter_genre,
    )
    return [PartialFilmInfo(**film.dict()) for film in out]


@router.get("/{film_id}", response_model=FilmFullInfo)
@cache(expire=REDIS_CACHE_EXPIRE)
async def film_details(
    request: Request,
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service),
) -> FilmFullInfo:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=FILM_NOT_FOUND)
    return FilmFullInfo(**film.dict())
