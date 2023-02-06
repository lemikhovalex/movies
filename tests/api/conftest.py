import asyncio
import os
import sys
from dataclasses import dataclass
from http import HTTPStatus
from typing import AsyncGenerator, Optional

import aiohttp
import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from multidict import CIMultiDictProxy
from settings import TestSettings
from test_data import constants, genres, movies, persons
from utils import es_load

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

SETTINGS = TestSettings()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def es_client() -> AsyncGenerator[AsyncElasticsearch, None]:
    """
    Создаёт файл и удаляет его, даже если сам тест упал в ошибку
    """
    url = f"http://{SETTINGS.es_host}:{SETTINGS.es_port}"
    es = AsyncElasticsearch(url)
    indecies = ["genres", "persons", "movies"]
    es = es.options(ignore_status=HTTPStatus.BAD_REQUEST)
    await asyncio.gather(
        *[
            es.indices.create(
                index=idx,
                settings=constants.settings,
                mappings=getattr(constants, f"mappings_{idx}"),
            )
            for idx in indecies
        ]
    )

    yield es

    es = es.options(ignore_status=[HTTPStatus.BAD_REQUEST, HTTPStatus.NOT_FOUND])
    await asyncio.gather(
        *[
            es.indices.delete(
                index=idx,
            )
            for idx in indecies
        ]
    )

    await es.close()


@pytest_asyncio.fixture(scope="module")
async def es_load_data(es_client) -> None:
    idx_data_map = (
        ("genres", genres.genres),
        ("movies", movies.movies),
        ("persons", persons.persons),
    )

    await asyncio.gather(
        *[es_load(es_client, index, data) for index, data in idx_data_map]
    )

    yield

    await es_client.delete_by_query(
        index=[i[0] for i in idx_data_map], query={"match_all": {}}
    )
    await asyncio.gather(
        *[es_client.indices.refresh(index=index) for index, _ in idx_data_map]
    )


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


@pytest_asyncio.fixture(scope="session")
async def session(es_client):  # use es_client arg to call index creation fixture
    session = aiohttp.ClientSession(headers={"Cache-Control": "no-store"})
    yield session
    await session.close()


@pytest_asyncio.fixture(scope="module")
def make_get_request_empty_es(session):
    """Request maker without preloaded es data fixtures"""

    async def inner(method: str, params: Optional[dict] = None) -> HTTPResponse:
        params = params or {}
        url = f"http://{SETTINGS.api_host}:{SETTINGS.api_port}/api/v1/{method.lstrip('/')}"  # noqa: E501
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner


@pytest_asyncio.fixture(scope="module")
def make_get_request(make_get_request_empty_es, es_load_data):
    """Request maker with preloaded es data fixtures"""
    return make_get_request_empty_es
