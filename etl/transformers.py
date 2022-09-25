import logging
from typing import List

import pandas as pd

from .data_structures import ESPerson, MergedFromPg, ToES
from .etl_interfaces import ITransformer
from .utils import process_exception

LOGGER_NAME = "transformer.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))


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
            ESPerson(id=p["person_id"], name=p["person_full_name"])
            for p in list_of_p
        ]
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
            movie_data = {
                "film_work_id": movie_id,
                "imdb_rating": post_process_nan(
                    movie_df["imdb_rating"].values[0],
                ),
                "genre_name": movie_df["genre_name"].unique().tolist(),
                "title": post_process_nan(movie_df["title"].values[0]),
                "description": post_process_nan(
                    movie_df["description"].values[0],
                ),
                "actors": actors,
                "writers": writers,
                "directors": [direct.name for direct in directors],
                "actors_names": [act.name for act in actors],
                "writers_names": [writ.name for writ in writers],
            }
            # if pydantic cant validate let's log it
            try:
                out.append(ToES.parse_obj(movie_data))
            except Exception as excep:
                process_exception(excep, logger)
        return out

    def save_state(self):
        pass
