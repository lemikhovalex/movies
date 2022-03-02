from abc import ABC, abstractmethod
from typing import ClassVar, Tuple

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
    def load(self):
        pass


class IPEMExtracter(IExtracter, ABC):
    state: State

    @abstractmethod
    def produce(self) -> Tuple[list, bool]:
        pass

    @abstractmethod
    def enrich(self, ids: list) -> Tuple[list, bool]:
        pass

    @abstractmethod
    def merge(self, ids: list) -> Tuple[list, bool]:
        pass

    def extract(self) -> Tuple[list, bool]:
        is_all_produced = False
        while not is_all_produced:
            proxy_ids, is_all_produced = self.produce()
            is_all_enriched = False
            while not is_all_enriched:
                target_ids, is_all_enriched = self.enrich(proxy_ids)
                is_all_merged = False
                while not is_all_merged:
                    return self.merge(target_ids)
        return ([], True)


class IETL(ABC):
    state: State

    extracter: IExtracter
    transformer: ITransformer
    loader: ILoader
