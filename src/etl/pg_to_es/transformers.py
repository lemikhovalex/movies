import logging
import os
from typing import List

import pandas as pd

from etl.config import CONFIG
from etl.pg_to_es.base import ITransformer
from etl.pg_to_es.data_structures import ESGenre, ESPerson, MergedFromPg, ToES
from etl.utils import process_exception

if CONFIG.logger_path is not None:
    LOGGER_NAME = os.path.join(CONFIG.logger_path, "transformer.log")
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(logging.FileHandler(LOGGER_NAME))
else:
    logger = logging


def filter_persons(df: pd.DataFrame, role: str) -> List[ESPerson]:
    persons = df[["person_id", "person_full_name", "role"]]
    list_of_p = (
        persons.query(
            "role == '{role}'".format(role=role),
        )
        .drop(columns=["role"])
        .drop_duplicates()
        .to_dict("records")
    )
    # if we cant process data we
    try:
        out = [
            ESPerson(id=p["person_id"], name=p["person_full_name"]) for p in list_of_p
        ]
    except Exception as excep:
        process_exception(excep, logger)
    return out


def get_genres(df: pd.DataFrame) -> List[ESGenre]:
    genres = df[["genre_name", "genre_id"]]
    list_of_genres = genres.drop_duplicates().to_dict("records")
    try:
        out = [ESGenre(id=g["genre_id"], name=g["genre_name"]) for g in list_of_genres]
    except Exception as excep:
        process_exception(excep, logger)
    return out


def post_process_nan(v):
    if pd.isna(v):
        return None
    else:
        return v


class PgToESTransformer(ITransformer):
    def transform(self, merged_data: List[MergedFromPg]) -> List[ToES]:
        if len(merged_data) == 0:
            return []
        df = pd.DataFrame(merged_data)
        movies_ids = df["film_work_id"].unique()
        out: List[ToES] = []
        for movie_id in movies_ids:
            movie_df = df.query(
                "film_work_id == '{fwid}'".format(fwid=movie_id),
            )
            writers = filter_persons(movie_df, "writer")
            actors = filter_persons(movie_df, "actor")
            directors = filter_persons(movie_df, "director")
            genres = get_genres(movie_df)
            movie_data = {
                "film_work_id": movie_id,
                "imdb_rating": post_process_nan(
                    movie_df["imdb_rating"].values[0],
                ),
                "title": post_process_nan(movie_df["title"].values[0]),
                "description": post_process_nan(
                    movie_df["description"].values[0],
                ),
                "genres_names": [g.name for g in genres],
                "directors_names": [direct.name for direct in directors],
                "actors_names": [act.name for act in actors],
                "writers_names": [writ.name for writ in writers],
                "actors": actors,
                "writers": writers,
                "directors": directors,
                "genres": genres,
            }
            # if pydantic cant validate let's log it
            try:
                out.append(ToES.parse_obj(movie_data))
            except Exception as excep:
                process_exception(excep, logger)
        return out

    def save_state(self):
        pass
