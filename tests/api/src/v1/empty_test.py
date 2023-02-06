from http import HTTPStatus

import pytest

# All test coroutines will be treated as marked with this decorator.
pytestmark = pytest.mark.asyncio


async def test_films_check_no_films(make_get_request_empty_es):
    response = await make_get_request_empty_es(
        "films",
        {
            "sort": "imdb_rating",
            "page[size]": 1000,
            "page[number]": 1,
        },
    )

    assert response.status == HTTPStatus.OK
    assert response.body == []


async def test_genres_empty(make_get_request_empty_es):
    response = await make_get_request_empty_es("genres")

    assert response.status == HTTPStatus.OK
    assert response.body == []


async def test_persons_empty(make_get_request_empty_es):
    response = await make_get_request_empty_es("persons")

    assert response.status == HTTPStatus.OK
    assert response.body == []
