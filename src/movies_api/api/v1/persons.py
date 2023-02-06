from http import HTTPStatus
from uuid import UUID

from api.v1 import PartialFilmInfo, PersonPartial, get_page_params
from api.v1.messages import PERSON_NOT_FOUND
from core.config import REDIS_CACHE_EXPIRE
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi_cache.decorator import cache
from services.films import FilmService, get_film_service
from services.persons import PersonService, get_person_service

router = APIRouter()


@router.get("/search", response_model=list[PersonPartial])
@cache(expire=REDIS_CACHE_EXPIRE)
async def persons_search(
    request: Request,
    query: str,
    page: dict = Depends(get_page_params),
    person_service: PersonService = Depends(get_person_service),
) -> list[PersonPartial]:
    persons = await person_service.get_by(
        name_part=query,
        page_number=page["number"],
        page_size=page["size"],
    )
    return [PersonPartial(**person.dict()) for person in persons]


@router.get("/{person_id}", response_model=PersonPartial)
@cache(expire=REDIS_CACHE_EXPIRE)
async def person_details(
    request: Request,
    person_id: UUID,
    person_service: PersonService = Depends(get_person_service),
) -> PersonPartial:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=PERSON_NOT_FOUND)
    return PersonPartial(**person.dict())


@router.get("", response_model=list[PersonPartial])
@cache(expire=REDIS_CACHE_EXPIRE)
async def persons(
    request: Request,
    page: dict = Depends(get_page_params),
    person_service: PersonService = Depends(get_person_service),
) -> list[PersonPartial]:
    persons = await person_service.get_by(
        page_number=page["number"],
        page_size=page["size"],
    )
    return [PersonPartial(**person.dict()) for person in persons]


@router.get("/{person_id}/films", response_model=list[PartialFilmInfo])
@cache(expire=REDIS_CACHE_EXPIRE)
async def person_films(
    request: Request,
    person_id: str,
    page: dict = Depends(get_page_params),
    film_service: FilmService = Depends(get_film_service),
) -> list[PartialFilmInfo]:
    films = await film_service.get_by(
        person_id=person_id,
        page_number=page["number"],
        page_size=page["size"],
        sort="-imdb_rating",
    )
    return [PartialFilmInfo(**film.dict()) for film in films]
