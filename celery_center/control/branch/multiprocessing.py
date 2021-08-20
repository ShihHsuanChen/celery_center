import multiprocessing
from typing import Optional, Any, Iterable, Mapping

from .base import Branch


class MultiprocessingBranch(Branch):
    def __init__(self,
            target,
            args: Iterable[Any],
            kwargs: Mapping[str, Any],
            **options
            ):

        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.options = options
        self._p = None

    def start(self):
        self._p = multiprocessing.Process(
            target=self.target,
            args=self.args,
            kwargs=self.kwargs,
            **self.options
        )
        self._p.start()

    def is_alive(self):
        return self._p is not None and self._p.is_alive()

    def join(self, timeout: Optional[int] = None):
        if self._p is not None:
            self._p.join(timeout=timeout)

    def terminate(self):
        if self._p is not None:
            self._p.terminate()
