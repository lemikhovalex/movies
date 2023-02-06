from uuid import UUID

from orjson import loads as orjson_loads
from pydantic import BaseModel as PydanticBaseModel

from .utils import orjson_dumps


class BaseModel(PydanticBaseModel):
    uuid: UUID

    class Config:
        fields = {"uuid": "id"}
        json_loads = orjson_loads
        json_dumps = orjson_dumps
