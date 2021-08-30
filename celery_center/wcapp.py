from typing import Any, Type, Mapping
from functools import wraps
from celery import Task, Celery

from .control import WorkerControlCenter
from .control.base import WorkspaceBase
from .control.tasks import WorkerControlTask, celery_center
from .control import tasks


def create_wcapp(
        app_name: str,
        broker=None,
        backend=None,
        workspace_cls: Type[WorkspaceBase] = WorkerControlCenter,
        workspace_kwargs: Mapping[str, Any] = dict(),
        **kwargs
        ) -> Celery:

    workspace_kwargs['app_name'] = app_name

    celery_center.add_workspace(
        workspace_cls,
        [WorkerControlTask],
        default_kwargs=workspace_kwargs
    )
    wcapp = celery_center.create_celery(
        broker=broker,
        backend=backend,
        worker_pool='solo',
        worker_concurrency=1,
        **kwargs,
    )

    def binded_task(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs.setdefault('base', WorkerControlTask)
            kwargs.setdefault('bind', True)
            return func(*args, **kwargs)
        return wrapper

    wcapp.task = binded_task(wcapp.task)
    return wcapp
