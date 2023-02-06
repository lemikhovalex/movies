from http import HTTPStatus

import pytest
from utils import filter_int, filter_uuid

# All test coroutines will be treated as marked with this decorator.
pytestmark = pytest.mark.asyncio


async def test_person_by_id_absent(make_get_request):
    response = await make_get_request("persons/1f64ea08-0000-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.NOT_FOUND


async def test_person_by_id(make_get_request):
    response = await make_get_request("persons/1f64ea08-b298-11ec-90b3-00155db24537")

    assert response.status == HTTPStatus.OK
    assert response.body == {
        "uuid": "1f64ea08-b298-11ec-90b3-00155db24537",
        "full_name": "Multitask person 0",
    }


async def test_persons(make_get_request):
    response = await make_get_request("persons")

    assert response.status == HTTPStatus.OK
    assert isinstance(response.body, list)
    assert len(response.body) == 21


async def test_persons_films(make_get_request):
    response = await make_get_request(
        "persons/1f64ea08-b298-11ec-90b3-00155db24537/films"
    )

    assert response.status == HTTPStatus.OK
    assert isinstance(response.body, list)
    assert len(response.body) == 6
    assert filter_uuid(response.body) == {
        "1f650754-b298-11ec-90b3-00155db24537",
        "1f651c76-b298-11ec-90b3-00155db24537",
        "1f652f72-b298-11ec-90b3-00155db24537",
        "1f6546ba-b298-11ec-90b3-00155db24537",
        "1f656672-b298-11ec-90b3-00155db24537",
        "1f657e5a-b298-11ec-90b3-00155db24537",
    }


@pytest.mark.parametrize(
    "page_num,page_size,resp_len",
    [
        (1, 3, 3),
        (2, 3, 3),
        (3, 3, 0),
    ],
    ids=filter_int,
)
async def test_persons_films_pagination(
    page_num, page_size, resp_len, make_get_request
):
    response = await make_get_request(
        "persons/1f64ea08-b298-11ec-90b3-00155db24537/films",
        {"page[size]": page_size, "page[number]": page_num},
    )
    assert response.status == HTTPStatus.OK
    assert isinstance(response.body, list)
    assert len(response.body) == resp_len


@pytest.mark.parametrize(
    "page_num,page_size,resp_len",
    [
        (1, 20, 20),
        (2, 20, 1),
    ],
    ids=filter_int,
)
async def test_persons_pagination(page_num, page_size, resp_len, make_get_request):
    response = await make_get_request(
        "persons", {"page[size]": page_size, "page[number]": page_num}
    )
    assert response.status == HTTPStatus.OK
    assert isinstance(response.body, list)
    assert len(response.body) == resp_len
