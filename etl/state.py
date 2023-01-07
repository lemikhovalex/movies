import abc
import json
from typing import Any


class BaseStateStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStateStorage):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        out = {}
        try:
            with open(self.file_path, "r") as f:
                out = json.load(f)
        except FileNotFoundError:
            pass
        return out

    def save_state(self, state: dict):
        with open(self.file_path, "w") as f:
            json.dump(state, f)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не
    перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с
    БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStateStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        out = None
        try:
            out = state[key]
        except KeyError:
            pass
        return out
