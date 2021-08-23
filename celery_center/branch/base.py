import abc
from typing import Optional


class Branch(abc.ABC):
    @abc.abstractmethod
    def start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def join(self, timeout: Optional[int] = None):
        raise NotImplementedError

    @abc.abstractmethod
    def is_alive(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def terminate(self):
        raise NotImplementedError
