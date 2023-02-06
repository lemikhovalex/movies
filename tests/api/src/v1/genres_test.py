from http import HTTPStatus

import pytest
from utils import filter_int, filter_uuid

# All test coroutines will be treated as marked with this decorator.
pytestmark = pytest.mark.asyncio
TEST_GENRES_PAGINATION_DATA = [
    (
        1,
        2,
        {
            "1f64e56c-b298-11ec-90b3-00155db24537",
            "1f64e81e-b298-11ec-90b3-00155db24537",
        },
    ),
    (
        2,
        2,
        {"1f64e918-b298-11ec-90b3-00155db24537"},
    ),
]

TEST_FILMS_BY_GENRES_PAGINATION_DATA = [
    (
        1,
        2,
        {
            "1f652f72-b298-11ec-90b3-00155db24537",
            "1f651c76-b298-11ec-90b3-00155db24537",
        },
    ),
    (
        2,
        2,
        {"1f657e5a-b298-11ec-90b3-00155db24537"},
    ),
]


async def test_genre_by_id_absent(make_get_request):
    response = await make_get_request("genres/1f64e918-0000-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.NOT_FOUND


async def test_genre_by_id(make_get_request):
    response = await make_get_request("genres/1f64e918-b298-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.OK
    assert response.body == {
        "uuid": "1f64e918-b298-11ec-90b3-00155db24537",
        "name": "action",
    }


async def test_genres(make_get_request):
    response = await make_get_request("genres")

    assert response.status == HTTPStatus.OK
    assert isinstance(response.body, list)
    assert len(response.body) == 3


async def test_genre_films(make_get_request):
    response = await make_get_request(
        "genres/1f64e918-b298-11ec-90b3-00155db24537/films"
    )

    assert response.status == HTTPStatus.OK
    assert filter_uuid(response.body) == {
        "1f652f72-b298-11ec-90b3-00155db24537",
        "1f651c76-b298-11ec-90b3-00155db24537",
        "1f657e5a-b298-11ec-90b3-00155db24537",
    }


@pytest.mark.parametrize(
    "page_num,page_size,expected_resp",
    TEST_GENRES_PAGINATION_DATA,
    ids=filter_int,
)
async def test_genres_pagination(page_num, page_size, expected_resp, make_get_request):
    response = await make_get_request(
        "genres", {"page[size]": page_size, "page[number]": page_num}
    )

    assert response.status == HTTPStatus.OK
    assert filter_uuid(response.body) == expected_resp


@pytest.mark.parametrize(
    "page_num,page_size,expected_resp",
    TEST_FILMS_BY_GENRES_PAGINATION_DATA,
    ids=filter_int,
)
async def test_genre_films_pagination(
    page_num, page_size, expected_resp, make_get_request
):
    response = await make_get_request(
        "genres/1f64e918-b298-11ec-90b3-00155db24537/films",
        {"page[size]": page_size, "page[number]": page_num},
    )

    assert response.status == HTTPStatus.OK
    assert filter_uuid(response.body) == expected_resp
