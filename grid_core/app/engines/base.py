from abc import ABC, abstractmethod


class BaseGridEngine(ABC):
    @abstractmethod
    def locate_point(self, lon: float, lat: float, level: int):
        raise NotImplementedError

    @abstractmethod
    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        raise NotImplementedError

    @abstractmethod
    def code_to_geometry(self, code: str):
        raise NotImplementedError

    @abstractmethod
    def code_to_center(self, code: str):
        raise NotImplementedError

    @abstractmethod
    def neighbors(self, code: str, k: int = 1):
        raise NotImplementedError

    @abstractmethod
    def parent(self, code: str):
        raise NotImplementedError

    @abstractmethod
    def children(self, code: str, target_level: int):
        raise NotImplementedError
