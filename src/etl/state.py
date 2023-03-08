import abc
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Iterable, Sequence

import redis
from more_itertools import chunked

from etl.config import CONFIG

if CONFIG.logger_path is not None:
    LOGGER_NAME = os.path.join(CONFIG.logger_path, "state.log")
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(logging.FileHandler(LOGGER_NAME))
else:
    logger = logging


class BaseStateStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """save state to storage"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """get state from storage"""
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
    Class  to work with records of internal processes.
    For example to avoid ETL start from beginning.
    This is realization with storing data to file.
    """

    def __init__(self, storage: BaseStateStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        state = self.storage.retrieve_state()
        out = None
        try:
            out = state[key]
        except KeyError:
            pass
        return out


class BaseUniqueStorage(ABC):
    @abstractmethod
    def update(self, items: Sequence[Any]) -> None:
        ...

    @abstractmethod
    def __len__(self) -> int:
        ...

    @abstractmethod
    def get_iterator(self, batch_size: int) -> Iterable[Sequence[Any]]:
        ...


class GenericQueue(BaseUniqueStorage):
    def __init__(self) -> None:
        self._storage = set()

    def update(self, items: Sequence[Any]) -> None:
        logger.info("GenericQueue::Gonna update state with")
        self._storage.update(items)
        logger.info("GenericQueue::State succesively updated")

    def get_iterator(self, batch_size: int) -> Iterable[Sequence[Any]]:
        ch_iter = chunked(self._storage, n=batch_size)
        for batch in ch_iter:
            yield batch

    def __len__(self) -> int:
        return len(self._storage)


class RedisQueue(BaseUniqueStorage):
    def __init__(self, q_name: str, conn: redis.Redis) -> None:
        self._storage = set()
        self._q_name = q_name
        self.conn = conn

    def update(self, items: Sequence[Any]) -> None:
        if len(items) > 0:
            self.conn.sadd(self._q_name, *items)

    def get_iterator(self, batch_size: int) -> Iterable[Sequence[Any]]:
        cursor = 0
        k = 0
        while (cursor != 0) or (k == 0):
            k += 1
            cursor, values = self.conn.sscan(
                name=self._q_name,
                cursor=cursor,
                count=batch_size,
            )
            yield values

    def __len__(self) -> int:
        return self.conn.scard(self._q_name)
