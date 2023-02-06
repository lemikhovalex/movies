from http import HTTPStatus

import pytest
from test_data import movies
from utils import filter_int, filter_uuid

# All test coroutines will be treated as marked with this decorator.
pytestmark = pytest.mark.asyncio


TEST_FILMS_PAGINATION_DATA = [
    (
        1,
        3,
        {
            "1f650754-b298-11ec-90b3-00155db24537",
            "1f656672-b298-11ec-90b3-00155db24537",
            "1f651c76-b298-11ec-90b3-00155db24537",
        },
    ),
    (
        2,
        3,
        {
            "1f657e5a-b298-11ec-90b3-00155db24537",
            "1f652f72-b298-11ec-90b3-00155db24537",
            "1f6546ba-b298-11ec-90b3-00155db24537",
        },
    ),
]


async def test_film_by_id_absent(make_get_request):
    response = await make_get_request("films/1f6546ba-0000-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.NOT_FOUND


async def test_film_by_id(make_get_request):
    response = await make_get_request("films/1f6546ba-b298-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.OK
    assert response.body == movies.expected_film_by_id


@pytest.mark.parametrize(
    "page_num,page_size,expected_resp", TEST_FILMS_PAGINATION_DATA, ids=filter_int
)
async def test_films_pagination(page_num, page_size, expected_resp, make_get_request):
    films_resp = await make_get_request(
        "films",
        params={
            "sort": "imdb_rating",
            "page[size]": page_size,
            "page[number]": page_num,
        },
    )

    assert films_resp.status == HTTPStatus.OK
    assert filter_uuid(films_resp.body) == expected_resp


async def test_films_check_all_films(make_get_request):
    resp_all_films = await make_get_request(
        "films", params={"sort": "imdb_rating", "page[size]": 1000, "page[number]": 1}
    )

    assert resp_all_films.status == HTTPStatus.OK
    assert isinstance(resp_all_films.body, list)
    assert len(resp_all_films.body) == 6
    assert resp_all_films.body == movies.expected_films_check_all
