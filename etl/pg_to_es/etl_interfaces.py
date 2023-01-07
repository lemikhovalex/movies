from abc import ABC, abstractmethod
from typing import ClassVar, Generator

from etl.state import State


class WithQuery(ABC):
    select_query: ClassVar[str]


class IExtracter(ABC):
    state: State

    @abstractmethod
    def extract(self) -> Generator[list, None, None]:
        pass

    @abstractmethod
    def save_state(self):
        pass


class ITransformer(ABC):
    state: State

    @abstractmethod
    def transform(self, data: list) -> list:
        pass

    @abstractmethod
    def save_state(self):
        pass


class ILoader(ABC):
    state: State

    @abstractmethod
    def load(self, data_to_load: list):
        pass

    @abstractmethod
    def save_state(self):
        pass


class IETL(ABC):
    state: State

    extracter: IExtracter
    transformer: ITransformer
    loader: ILoader

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def save_state(self):
        pass
