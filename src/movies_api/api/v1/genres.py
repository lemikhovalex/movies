from http import HTTPStatus
from uuid import UUID

from api.v1 import GenrePartial, PartialFilmInfo, get_page_params
from api.v1.messages import GENRE_NOT_FOUND
from core.config import REDIS_CACHE_EXPIRE
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi_cache.decorator import cache
from services.films import FilmService, get_film_service
from services.genres import GenreService, get_genre_service

router = APIRouter()


@router.get("/{genre_id}", response_model=GenrePartial)
@cache(expire=REDIS_CACHE_EXPIRE)
async def genre_details(
    request: Request,
    genre_id: UUID,
    genre_service: GenreService = Depends(get_genre_service),
) -> GenrePartial:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=GENRE_NOT_FOUND)
    return GenrePartial(**genre.dict())


@router.get("", response_model=list[GenrePartial])
@cache(expire=REDIS_CACHE_EXPIRE)
async def genres(
    request: Request,
    page: dict = Depends(get_page_params),
    genre_service: GenreService = Depends(get_genre_service),
) -> list[GenrePartial]:
    genres = await genre_service.get_by(
        page_number=page["number"],
        page_size=page["size"],
    )
    return [GenrePartial(**genre.dict()) for genre in genres]


@router.get("/{genre_id}/films", response_model=list[PartialFilmInfo])
@cache(expire=REDIS_CACHE_EXPIRE)
async def genre_films(
    request: Request,
    genre_id: str,
    page: dict = Depends(get_page_params),
    film_service: FilmService = Depends(get_film_service),
) -> list[PartialFilmInfo]:
    films = await film_service.get_by(
        genre_id=genre_id,
        page_number=page["number"],
        page_size=page["size"],
        sort="-imdb_rating",
    )
    return [PartialFilmInfo(**film.dict()) for film in films]
