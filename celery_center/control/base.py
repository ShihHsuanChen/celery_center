import abc
from typing import List, Mapping, Any
from click import Option


class WorkspaceBase(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def options(cls, defaults: Mapping[str, Any] = dict()) -> List[Option]:
        raise NotImplementedError
    
    @classmethod
    @abc.abstractmethod
    def register_workspace(cls) -> object:
        raise NotImplementedError

    @abc.abstractmethod
    def terminate(self):
        raise NotImplementedError
