import copy
import json
from operator import attrgetter

from pydantic import BaseModel


class PartialFilm(BaseModel):
    imdb_rating: float
    title: str
    uuid: str

    class Config:
        fields = {"uuid": "id"}


movies = [
    {
        "id": "1f650754-b298-11ec-90b3-00155db24537",
        "imdb_rating": 10.0,
        "title": "HP 1",
        "description": "description for HP",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e56c-b298-11ec-90b3-00155db24537", "name": "comedy"},
            {"id": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
        ],
        "actors": [
            {"id": "1f64f02a-b298-11ec-90b3-00155db24537", "name": "HP_actor_0"},
            {"id": "1f64f138-b298-11ec-90b3-00155db24537", "name": "HP_actor_1"},
            {"id": "1f64f228-b298-11ec-90b3-00155db24537", "name": "HP_actor_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
        "directors": [
            {"id": "1f64f674-b298-11ec-90b3-00155db24537", "name": "HP_director_0"},
            {"id": "1f64f746-b298-11ec-90b3-00155db24537", "name": "HP_director_1"},
            {"id": "1f64f822-b298-11ec-90b3-00155db24537", "name": "HP_director_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
        "writers": [
            {"id": "1f64f32c-b298-11ec-90b3-00155db24537", "name": "HP_writer_0"},
            {"id": "1f64f412-b298-11ec-90b3-00155db24537", "name": "HP_writer_1"},
            {"id": "1f64f520-b298-11ec-90b3-00155db24537", "name": "HP_writer_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
    },
    {
        "id": "1f651c76-b298-11ec-90b3-00155db24537",
        "imdb_rating": 9.9,
        "title": "HP 2",
        "description": "description for HP",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
            {"id": "1f64e918-b298-11ec-90b3-00155db24537", "name": "action"},
        ],
        "actors": [
            {"id": "1f64f02a-b298-11ec-90b3-00155db24537", "name": "HP_actor_0"},
            {"id": "1f64f138-b298-11ec-90b3-00155db24537", "name": "HP_actor_1"},
            {"id": "1f64f228-b298-11ec-90b3-00155db24537", "name": "HP_actor_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
        "directors": [
            {"id": "1f64f674-b298-11ec-90b3-00155db24537", "name": "HP_director_0"},
            {"id": "1f64f746-b298-11ec-90b3-00155db24537", "name": "HP_director_1"},
            {"id": "1f64f822-b298-11ec-90b3-00155db24537", "name": "HP_director_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
        "writers": [
            {"id": "1f64f32c-b298-11ec-90b3-00155db24537", "name": "HP_writer_0"},
            {"id": "1f64f412-b298-11ec-90b3-00155db24537", "name": "HP_writer_1"},
            {"id": "1f64f520-b298-11ec-90b3-00155db24537", "name": "HP_writer_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
    },
    {
        "id": "1f652f72-b298-11ec-90b3-00155db24537",
        "imdb_rating": 9.8,
        "title": "HP 3",
        "description": "description for HP",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e918-b298-11ec-90b3-00155db24537", "name": "action"},
            {"id": "1f64e56c-b298-11ec-90b3-00155db24537", "name": "comedy"},
        ],
        "actors": [
            {"id": "1f64f02a-b298-11ec-90b3-00155db24537", "name": "HP_actor_0"},
            {"id": "1f64f138-b298-11ec-90b3-00155db24537", "name": "HP_actor_1"},
            {"id": "1f64f228-b298-11ec-90b3-00155db24537", "name": "HP_actor_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
        "directors": [
            {"id": "1f64f674-b298-11ec-90b3-00155db24537", "name": "HP_director_0"},
            {"id": "1f64f746-b298-11ec-90b3-00155db24537", "name": "HP_director_1"},
            {"id": "1f64f822-b298-11ec-90b3-00155db24537", "name": "HP_director_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
        "writers": [
            {"id": "1f64f32c-b298-11ec-90b3-00155db24537", "name": "HP_writer_0"},
            {"id": "1f64f412-b298-11ec-90b3-00155db24537", "name": "HP_writer_1"},
            {"id": "1f64f520-b298-11ec-90b3-00155db24537", "name": "HP_writer_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
    },
    {
        "id": "1f6546ba-b298-11ec-90b3-00155db24537",
        "imdb_rating": 9.7,
        "title": "HP 4",
        "description": "description for HP",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e56c-b298-11ec-90b3-00155db24537", "name": "comedy"},
            {"id": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
        ],
        "actors": [
            {"id": "1f64f02a-b298-11ec-90b3-00155db24537", "name": "HP_actor_0"},
            {"id": "1f64f138-b298-11ec-90b3-00155db24537", "name": "HP_actor_1"},
            {"id": "1f64f228-b298-11ec-90b3-00155db24537", "name": "HP_actor_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
        "directors": [
            {"id": "1f64f674-b298-11ec-90b3-00155db24537", "name": "HP_director_0"},
            {"id": "1f64f746-b298-11ec-90b3-00155db24537", "name": "HP_director_1"},
            {"id": "1f64f822-b298-11ec-90b3-00155db24537", "name": "HP_director_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
        "writers": [
            {"id": "1f64f32c-b298-11ec-90b3-00155db24537", "name": "HP_writer_0"},
            {"id": "1f64f412-b298-11ec-90b3-00155db24537", "name": "HP_writer_1"},
            {"id": "1f64f520-b298-11ec-90b3-00155db24537", "name": "HP_writer_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
    },
    {
        "id": "1f656672-b298-11ec-90b3-00155db24537",
        "imdb_rating": 10.0,
        "title": "SW 1",
        "description": "description for SW",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e56c-b298-11ec-90b3-00155db24537", "name": "comedy"},
            {"id": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
        ],
        "actors": [
            {"id": "1f654e26-b298-11ec-90b3-00155db24537", "name": "SW_actor_0"},
            {"id": "1f654f48-b298-11ec-90b3-00155db24537", "name": "SW_actor_1"},
            {"id": "1f655038-b298-11ec-90b3-00155db24537", "name": "SW_actor_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
        "directors": [
            {"id": "1f6553da-b298-11ec-90b3-00155db24537", "name": "SW_director_0"},
            {"id": "1f6554ca-b298-11ec-90b3-00155db24537", "name": "SW_director_1"},
            {"id": "1f6555b0-b298-11ec-90b3-00155db24537", "name": "SW_director_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
        "writers": [
            {"id": "1f65511e-b298-11ec-90b3-00155db24537", "name": "SW_writer_0"},
            {"id": "1f655204-b298-11ec-90b3-00155db24537", "name": "SW_writer_1"},
            {"id": "1f6552ea-b298-11ec-90b3-00155db24537", "name": "SW_writer_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
    },
    {
        "id": "1f657e5a-b298-11ec-90b3-00155db24537",
        "imdb_rating": 9.9,
        "title": "SW 2",
        "description": "description for SW",
        "directors_names": [],
        "actors_names": [],
        "writers_names": [],
        "genres": [
            {"id": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
            {"id": "1f64e918-b298-11ec-90b3-00155db24537", "name": "action"},
        ],
        "actors": [
            {"id": "1f654e26-b298-11ec-90b3-00155db24537", "name": "SW_actor_0"},
            {"id": "1f654f48-b298-11ec-90b3-00155db24537", "name": "SW_actor_1"},
            {"id": "1f655038-b298-11ec-90b3-00155db24537", "name": "SW_actor_2"},
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
        ],
        "directors": [
            {"id": "1f6553da-b298-11ec-90b3-00155db24537", "name": "SW_director_0"},
            {"id": "1f6554ca-b298-11ec-90b3-00155db24537", "name": "SW_director_1"},
            {"id": "1f6555b0-b298-11ec-90b3-00155db24537", "name": "SW_director_2"},
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
            {
                "id": "1f64ed64-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 1",
            },
        ],
        "writers": [
            {"id": "1f65511e-b298-11ec-90b3-00155db24537", "name": "SW_writer_0"},
            {"id": "1f655204-b298-11ec-90b3-00155db24537", "name": "SW_writer_1"},
            {"id": "1f6552ea-b298-11ec-90b3-00155db24537", "name": "SW_writer_2"},
            {
                "id": "1f64eecc-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 2",
            },
            {
                "id": "1f64ea08-b298-11ec-90b3-00155db24537",
                "name": "Multitask person 0",
            },
        ],
    },
]

expected_film_by_id = {
    "uuid": "1f6546ba-b298-11ec-90b3-00155db24537",
    "imdb_rating": 9.7,
    "title": "HP 4",
    "description": "description for HP",
    "genres": [
        {"uuid": "1f64e56c-b298-11ec-90b3-00155db24537", "name": "comedy"},
        {"uuid": "1f64e81e-b298-11ec-90b3-00155db24537", "name": "drama"},
    ],
    "actors": [
        {"uuid": "1f64f02a-b298-11ec-90b3-00155db24537", "full_name": "HP_actor_0"},
        {"uuid": "1f64f138-b298-11ec-90b3-00155db24537", "full_name": "HP_actor_1"},
        {"uuid": "1f64f228-b298-11ec-90b3-00155db24537", "full_name": "HP_actor_2"},
        {
            "uuid": "1f64ea08-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 0",
        },
        {
            "uuid": "1f64ed64-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 1",
        },
    ],
    "directors": [
        {
            "uuid": "1f64f674-b298-11ec-90b3-00155db24537",
            "full_name": "HP_director_0",
        },
        {
            "uuid": "1f64f746-b298-11ec-90b3-00155db24537",
            "full_name": "HP_director_1",
        },
        {
            "uuid": "1f64f822-b298-11ec-90b3-00155db24537",
            "full_name": "HP_director_2",
        },
        {
            "uuid": "1f64eecc-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 2",
        },
        {
            "uuid": "1f64ea08-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 0",
        },
    ],
    "writers": [
        {
            "uuid": "1f64f32c-b298-11ec-90b3-00155db24537",
            "full_name": "HP_writer_0",
        },
        {
            "uuid": "1f64f412-b298-11ec-90b3-00155db24537",
            "full_name": "HP_writer_1",
        },
        {
            "uuid": "1f64f520-b298-11ec-90b3-00155db24537",
            "full_name": "HP_writer_2",
        },
        {
            "uuid": "1f64ed64-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 1",
        },
        {
            "uuid": "1f64eecc-b298-11ec-90b3-00155db24537",
            "full_name": "Multitask person 2",
        },
    ],
}


def _expected_films_check_all():
    _MOVIES = copy.deepcopy(movies)
    _MOVIES = [PartialFilm(**m) for m in _MOVIES]
    _MOVIES = sorted(_MOVIES, key=attrgetter("uuid"))
    _MOVIES = sorted(_MOVIES, key=attrgetter("imdb_rating"), reverse=True)
    _MOVIES = [json.loads(f.json()) for f in _MOVIES]
    return _MOVIES


expected_films_check_all = _expected_films_check_all()
