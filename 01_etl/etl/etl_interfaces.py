from abc import ABC, abstractmethod
from typing import ClassVar, Generator, Tuple

from .state import State


class WithQuery(ABC):
    select_query: ClassVar[str]


class IExtracter(ABC):
    state: State

    @abstractmethod
    def extract(self):
        pass


class ITransformer(ABC):
    state: State

    @abstractmethod
    def transform(self, data: list) -> list:
        pass


class ILoader(ABC):
    state: State

    @abstractmethod
    def load(self, data_to_load: list):
        pass


class IPEMExtracter(IExtracter, ABC):
    state: State

    @abstractmethod
    def produce(self) -> Tuple[list, bool]:
        pass

    @abstractmethod
    def enrich(self, ids: list) -> list:
        pass

    @abstractmethod
    def merge(self, ids: list) -> list:
        pass

    def extract(self) -> Generator[list, None, None]:
        is_all_produced = False
        while not is_all_produced:
            proxy_ids, is_all_produced = self.produce()
            target_ids = self.enrich(proxy_ids)

            yield self.merge(target_ids)


class IETL(ABC):
    state: State

    extracter: IExtracter
    transformer: ITransformer
    loader: ILoader
