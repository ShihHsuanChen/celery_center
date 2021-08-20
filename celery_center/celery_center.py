import os
import abc
import sys
from functools import wraps
from typing import Callable, Optional, Dict, Any, Type, List, Mapping

from click import Option
from celery import Celery, Task
from celery.bootsteps import Step
from .control.base import WorkspaceBase


class TaskCenter:
    def __init__(self,
            func: Callable,
            task_kwargs: Dict[str, Any] = dict(),
            ):
        self._func = func
        self._task_kwargs = task_kwargs
        self._bind_func = None

    def add(self,
            celery_instance: Celery,
            task_mixin: Optional[Type] = None
            ):
        if task_mixin is not None and isinstance(task_mixin, type):
            base = self._task_kwargs.pop('base', Task)
            name = 'Binded' + base.__name__
            binded_base = type(name, (task_mixin, base), dict())
            self._task_kwargs['base'] = binded_base

        wrapper = celery_instance.task(**self._task_kwargs)
        bind_func = wrapper(self._func)
        self._bind_func = bind_func
        return bind_func

    def delay(self, *args, **kwargs):
        return self._bind_func.delay(*args, **kwargs)

    def apply_async(self, *args, **kwargs):
        return self._bind_func.apply_async(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


class AbstractCeleryCenterTask(abc.ABC):

    def __new__(cls, *args, worker_group=None, **task_kwargs):
        if len(args) > 0:
            func, *args = args
            obj = cls(*args, **task_kwargs)
            return obj(func)
        else:
            return super(AbstractCeleryCenterTask, cls).__new__(cls)

    def __init__(self, *args, worker_group=None, **task_kwargs):
        self.args = args
        self.kwargs = task_kwargs

    @property
    @abc.abstractmethod
    def celery_center(self):
        raise NotImplementedError

    def __call__(self, func):
        return self.celery_center.add_task(func, self.kwargs)

        
class CeleryCenter:
    def __init__(self):
        class CeleryCenterTask(AbstractCeleryCenterTask):
            celery_center = self
        self.CeleryCenterTask = CeleryCenterTask
        self._task_center_list = list()
        self._user_options = dict()
        self._workspaces = list()

    @property
    def task(self):
        return self.CeleryCenterTask

    def _register_tasks(self,
            celery_instance: Celery,
            task_mixin: Optional[Type] = None
            ):
        for task_center in self._task_center_list:
            task_center.add(celery_instance, task_mixin=task_mixin)

    def add_task(self, func: Callable, kwargs: Dict[str, Any]):
        task_center = TaskCenter(func, kwargs)
        self._task_center_list.append(task_center)
        return task_center

    def _register_worker_options(self, celery_instance: Celery):
        worker_options = self._user_options.get('worker')
        if worker_options is None or len(worker_options) == 0:
            return

        class CustomArgs(Step):
            def __init__(self, worker, **options):
                for info in worker_options:
                    kwargs = {
                        k: options.pop(k, default)
                        for k, default in info['kwargs'].items()
                    }
                    info['callback'](**kwargs)
                super(CustomArgs, self).__init__(worker, **options)

        celery_instance.steps['worker'].add(CustomArgs)
        for info in worker_options:
            for option in info['options']:
                celery_instance.user_options['worker'].add(option)

    def add_workspace(self,
            task_base: Type[Task],
            workspace_cls: Type[WorkspaceBase],
            default_kwargs: Mapping[str, Any] = dict(),
            ):
        if not issubclass(workspace_cls, WorkspaceBase):
            raise TypeError(
                'Argument `workspace_cls` should be a subclass of '
                '{WorkspaceBase}.'
            )
        #TODO duplicated warning?
        def _register_workspace(**kwargs):
            obj = workspace_cls.register_workspace(**kwargs)
            task_base.workspace = obj

        self.add_worker_options(
            workspace_cls.options(defaults=default_kwargs),
            callback=_register_workspace
        )
        self._workspaces.append((workspace_cls, task_base))

    def add_worker_options(self,
            options: List[Option],
            callback: Callable,
            ):
        if 'worker' not in self._user_options:
            self._user_options['worker'] = list()
        info = {
            'kwargs': {opt.name: opt.default for opt in options},
            'options': options,
            'callback': callback,
        }
        self._user_options['worker'].append(info)

    def _setdefault(self, kwargs, key, value):
        conf = kwargs.get('config_source')
        if conf is None or not hasattr(conf, key):
            kwargs.setdefault(key, value)

    def create_celery(self, app=None, **kwargs):
        defaults = {
            'worker_pool': 'threads'
        }
        r"""
        change default pool type to `threads` because `prefork` (default) 
        and `processes` cause runtime problem in torch model forward 
        (regardless of device)
        """
        if app is not None:
            defaults['main'] = app.import_name
            defaults['backend'] = app.config.get('result_backend')
            defaults['broker'] = app.config.get('CELERY_BROKER_URL')

        for k, v in defaults.items():
            self._setdefault(kwargs, k, v)

        celery_instance = Celery(**kwargs)

        if app is not None:
            class ContextTaskMixin:
                def __call__(self, *args, **kwargs):
                    with app.app_context():
                        return self.run(*args, **kwargs)

            celery_instance.conf.update(app.config)
            task_mixin = ContextTaskMixin
        else:
            task_mixin = None
        self._register_tasks(celery_instance, task_mixin=task_mixin)
        self._register_worker_options(celery_instance)
        return celery_instance

    def shutdown(self):
        if sys.argv[0].split(os.sep)[-1] == 'celery' and 'worker' in sys.argv:
            print('Execute shutdown handler...')
            for _, task_base in self._workspaces:
                if getattr(task_base, 'workspace', None) is not None:
                    task_base.workspace.terminate()
