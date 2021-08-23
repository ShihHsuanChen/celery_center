import subprocess
from typing import Optional, Any, Iterable

from .base import Branch


class SubprocessBranch(Branch):
    def __init__(self, args: Iterable[Any], **options):

        self.args = args
        self.options = options
        self._p = None

    def start(self):
        self._p = subprocess.Popen(
            args=self.args,
            **self.options
        )

    def is_alive(self):
        return self._p is not None and self._p.poll() is None

    def join(self, timeout: Optional[int] = None):
        if self._p is not None:
            self._p.wait(timeout=timeout)

    def terminate(self):
        if self._p is not None:
            self._p.terminate()
