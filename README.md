# celery_center

## Install
```shell=
$ pip install git+<git repo to celery_center>
```

## Register Tasks

1. Create object of CeleryCenter
2. Use CeleryCenter object as celery
3. Call `celery_center.create_celery(...)` to create `Celery` object

Example:
```python=
from celery import Task
from celery_center import CeleryCenter

celery_center = CeleryCenter()

@celery_center.task
def add_numbers(a, b):
    return a+b

@celery_center.task(bind=True)
def foo(task, name):
    return f'{name}-{task.request.id}'

class MyTaskBase(Task):
    base_name = 'MyTaskBase'

@celery_center.task(base=MyTaskBase, bind=True)
def goo(task, name):
    return f'{name}-{task.base_name}-{task.request.id}'

app = celery_center.create_celery(broker='redis://', backend='redis://')
```

## Tasks with Workspace

1. Create workspace class and implement the following methods

- options(cls, defaults=dict()) (*classmethod*)

    Define `user_options` of the worker which contains this workspace

- register\_workspace(cls, \*\*kwargs) (*classmethod*)

    Callback function when a worker is created with `user_options`. User should parse the input arguments `kwargs` and create the workspace object (if needed). This callback is called only one time when worker starts. All tasks with base containing this workspace share the workspace object.

- terminate(cls)

    Callback function when a worker is terminated.

Example:
```python=
from click import Option
from celery_center.control.base import WorkspaceBase


class MyWorkspace(Workspacebase):
    @classmethod
    def options(cls, defaults: dict):
        options = [
            Option(
                ('--model-name', 'model_name'),
                default=defaults.get('model_name', 'resnet50'),
                show_default=True,
                help='Model structure name'
            ),
            Option(
                ('--device', 'device'),
                default=defaults.get('device', 'cpu'),
                show_default=True,
                help='Use device'
            )
        ]
        return options
    
    @classmethod
    def register_workspace(cls, **kwargs):
        model_name = kwargs.get('model_name')
        if model_name is not None:
            device = kwargs.get('device')
            return cls(model_name, device=device)

    def __init__(self, model_name, device='cpu'):
        self.model_name = model_name
        self.device = device
        ...

    def predict(self, inputs):
        return ...

    def terminate(self):
        pass
```

2. Create task class for this workspace and register workspace

Example:
```python=
from celery import Task


class ModelTask(Task):
    workspace = None # the Workspace object puts here, fixed name `workspace`

celery_center.add_workspace(ModelTask, MyWorkspaces)
```

3. Register tasks with the ModelTask base

Example:
```python=
@celery_center.task(base=ModelTask, bind=True)
def predict(task, inputs):
    return task.workspace.predict(inputs)
```

## Worker control center
Create a celery worker to control celery workers.

### Basic Usage
Suppose that we have main celery tasks with celery app in `app.py`
```python=
# app.py
from celery_center import CeleryCenter

celery_center = CeleryCenter()
...
celery = celery_center.create_celery(...)
```
The app name would be 'app.celery'.

```python=
# control.py
from celery_center.wcapp import create_wcapp

wcapp = create_wcapp('app.celery')
```
run the control app

```shell=
$ celery -A control.wcapp worker -l INFO
```
Notice that the default value of the worker pool is set to be `solo` and the concurrency is set to `1` because we don't want the main worker creation and worker destruction messing up together. But one can still overwrite the setting by the command line options.

```shell=
$ celery -A control.wcapp worker -l INFO --pool threads
```

### Giving initial workers configuration in `json` file.
Suppose that we want to start with two workers: "node1" has queue `default` and "node2" has `default` and `long` with concurrency `2`, both of workers have the logging level `INFO`
```json=
# worker_cfg.json
{
    "global": {
        "workers": {
            "loglevel": "INFO"
        }
    },
    "workers": {
        "node1": {
            "queues": ["default"]
        },
        "node2": {
            "queues": ["default", "long"],
            "concurrency": 2
        }
    }
}
```
Run worker
```shell=
$ celery -A control.wcapp worker -l INFO --cfg-path worker_cfg.json
```
### More options
Check out `--help` for more options
```shell=
$ celery -A control.wcapp worker --help
```
