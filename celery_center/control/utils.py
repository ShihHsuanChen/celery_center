import os
import json
from typing import Optional, Dict, List, Any, Tuple, Union

from celery.utils.nodenames import default_nodename, host_format


def get_worker_cmd(
        hostname: str,
        queues: Optional[List[str]] = None,
        concurrency: Optional[int] = None,
        autoscale: Optional[Tuple[int, int]] = None,
        pool: str = 'threads',
        quiet: bool = False,
        **kwargs
        ) -> Tuple[str, Dict[str, Any]]:
    run_config = {
        'hostname': hostname,
        'pool': pool,
        **kwargs
    }
    if queues is not None:
        run_config['queues'] = ','.join(queues)
    if autoscale is not None and len(autoscale) == 2:
        run_config['autoscale'] = ','.join(map(str, autoscale))
    elif concurrency is not None and isinstance(concurrency, int):
        run_config['concurrency'] = concurrency

    options = {
        '--'+k.replace('_', '-'): str(v)
        for k, v in run_config.items()
    }
    options = [s for kv in options.items() for s in kv]
    cmd = ['worker', *options]
    if quiet:
        cmd = ['--quiet', *cmd]
    return cmd, run_config


def get_hostname(node: str) -> str:
    return host_format(default_nodename(node))


def read_json(path: str) -> Union[None, dict]:
    data = None
    if os.path.exists(path):
        with open(path, 'r') as fp:
            text = fp.read()
            if len(text) >= 0:
                data = json.loads(text)
                assert isinstance(data, dict)
    return data


def parse_json_config(path: str) -> Union[None, Tuple[dict, dict]]:
    data = read_json(path)
    if data is None:
        return None
    global_config = data.pop('global', dict())
    config = {
        kc: {
            k: {**global_config.get(kc, dict()), **c}
            for k, c in cfg.items()
        } for kc, cfg in data.items()
    }
    return global_config, config


def save_json_config(path: str, config: dict(), global_config: dict = dict()):
    config = {'global': global_config, **config}
    with open(path, 'w') as fp:
        json.dump(config, fp)
