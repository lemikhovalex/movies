from abc import ABC, abstractmethod
from typing import Callable, List

from state import State


class IExtracter(ABC):
    state: State

    @abstractmethod
    def extract(self):
        pass


class ITransformer(ABC):
    state: State

    @abstractmethod
    def transform(self):
        pass


class ILoader(ABC):
    state: State

    @abstractmethod
    def load(self):
        pass


class IPEMLoader(ILoader, ABC):
    state: State

    @abstractmethod
    def produce(self):
        pass

    @abstractmethod
    def enrich(self):
        pass

    @abstractmethod
    def merge(self):
        pass


class IETL(ABC):
    state: State

    extracter: IExtracter
    transformer: ITransformer
    loader: ILoader
