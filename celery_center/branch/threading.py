import threading
from typing import Optional, Any, Iterable, Mapping

from .base import Branch


class ThreadingBranch(Branch):
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
        self._th = None

    def start(self):
        self._th = threading.Thread(
            target=self.target,
            args=self.args,
            kwargs=self.kwargs,
            **self.options
        )
        self._th.start()

    def is_alive(self):
        return self._th is not None and self._th.is_alive()

    def join(self, timeout: Optional[int] = None):
        if self._th is not None:
            self._th.join(timeout=timeout)

    def terminate(self):
        if self._th is not None:
            self._th.join(0.1) #??
