import abc
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Iterable, Sequence

LOGGER_NAME = "logs/state.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))


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


class GenericFileStorage(BaseStateStorage):
    def __init__(self) -> None:
        self.data = dict()

    def retrieve_state(self) -> dict:
        return self.data

    def save_state(self, state: dict):
        ...


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


class BaseUniqueStorage(ABC):
    @abstractmethod
    def update(self, items: Iterable[Any]) -> None:
        ...

    @abstractmethod
    def pop(self, batch_size: int) -> Sequence[Any]:
        ...

    @abstractmethod
    def __len__(self) -> int:
        ...


class GenericQueue(BaseUniqueStorage):
    def __init__(self) -> None:
        self._storage = set()

    def update(self, items: Iterable[Any]) -> None:
        logger.info("GenericQueue::Gonna update state with")
        self._storage.update(items)
        logger.info("GenericQueue::State succesively updated")

    def pop(self, batch_size: int) -> Sequence[Any]:
        _batch_size = min(batch_size, len(self))

        logger.info(f"GenericQueue::gonna pop {_batch_size} items")
        return [self._storage.pop() for _ in range(_batch_size)]

    def __len__(self) -> int:
        return len(self._storage)
