import marshal
import base64

from typing import Callable, Any
from types import FunctionType
from functools import wraps
from celery import Task
from celery.app.control import Inspect

from ..celery_center import CeleryCenter
from .worker_control_center import WorkerControlCenter
from .base import WorkspaceBase


celery_center = CeleryCenter()


class WorkerControlTask(Task):
    workspace = None


def force_sync(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func.delay(*args, **kwargs).wait()
    return wrapper


def inspect(func: Callable[[Inspect], Any]):
    codebytes = marshal.dumps(func.__code__)
    codestr = base64.b64encode(codebytes).decode()
    return _inspect.delay(codestr).wait()


@force_sync
@celery_center.task(base=WorkerControlTask, bind=True, name='control.info')
def info(task):
    return task.workspace.info()


@celery_center.task(base=WorkerControlTask, bind=True, name='control._inspect')
def _inspect(task, codestr):
    codebytes = base64.b64decode(codestr.encode())
    func = FunctionType(marshal.loads(codebytes), globals())
    return func(task.workspace.inspect)


@force_sync
@celery_center.task(base=WorkerControlTask, bind=True, name='control.active_queue_names')
def active_queue_names(task):
    aq = task.workspace.inspect.active_queues()
    if aq is None:
        return list()
    return list({q['name'] for qlist in aq.values() for q in qlist})


@celery_center.task(base=WorkerControlTask, bind=True, name='control.create_worker')
def create_worker(task, node, kwargs=dict()):
    return task.workspace.start_worker(node, kwargs, wait_for_ready=True)


@celery_center.task(base=WorkerControlTask, bind=True, name='control.remove_worker')
def remove_worker(task, node):
    task.workspace.stop_workers(node, join=True)
