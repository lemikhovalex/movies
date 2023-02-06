from .base import BaseModel


class BasePerson(BaseModel):
    full_name: str

    class Config:
        fields = {"uuid": "id", "full_name": "name"}


class Person(BasePerson):
    roles: list[str]
