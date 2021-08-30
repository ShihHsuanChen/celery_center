import time
import json
from pprint import pprint
from typing import Optional, Union, Callable, Dict, List, Any, Type, Tuple, Iterable, Mapping
from collections import OrderedDict

from click import Option
from celery.app.utils import find_app
from celery.app.control import Control, Inspect

from celery_center.branch.subprocess import SubprocessBranch
from .base import WorkspaceBase
from .utils import get_worker_cmd, get_hostname
from .utils import parse_json_config, save_json_config


class WorkerProcess:

    def __init__(self,
            app_name: str,
            hostname: str,
            quiet: bool = True,
            **run_config
            ):
        self.app_name = app_name
        self.app = find_app(app_name)
        self.hostname = get_hostname(hostname)
        self.host, self.node = self.hostname.split('@')
        self.quiet = quiet
        self.cmd, self.config = get_worker_cmd(
            self.hostname,
            quiet=quiet,
            **run_config
        )
        self.full_cmd = ['celery', '-A', self.app_name, *self.cmd]
        self._p = SubprocessBranch(self.full_cmd)
        self._is_ready = False

    @property
    def is_running(self) -> bool:
        return self._p.is_alive()

    @property
    def is_ready(self) -> bool:
        return self.ping() is not None

    def ping(self):
        obj = self.app.control.ping([self.hostname])
        return None if len(obj) == 0 else obj[0].get(self.hostname)

    def wait_for_ready(self):
        while True:
            if not self.is_running:
                return False
            elif self.is_ready:
                return True
            time.sleep(0.2)

    def start(self):
        return self._p.start() if not self.is_running else None

    def info(self):
        info = {
            'hostname': self.hostname,
            'node': self.node,
            'host': self.host,
            'cmd': self.cmd,
            'full_cmd': self.full_cmd,
            'config': self.config,
            'is_running': self.is_running,
            'is_ready': self.is_ready,
        }
        return info

    def shutdown(self, join: bool = False, timeout: Optional[int] = None):
        if not self.is_running:
            return
        self.app.control.shutdown([self.hostname])
        if join:
            self.join(timeout=timeout)

    def join(self, timeout: Optional[int] = None):
        if not self.is_running:
            return
        return self._p.join(timeout=timeout)

    
class WorkerControlCenter(WorkspaceBase):
    @classmethod
    def options(cls, defaults: Mapping[str, Any] = dict()) -> List[Option]:
        options = [
            Option(
                ('--app-name', 'app_name'),
                default=defaults.get('app_name'),
                help='celery app module name'
            ),
            Option(
                ('--init-cfg', 'init_cfg'),
                default=defaults.get('init_cfg', None),
                type=str,
                help='initial config file in json format'
            ),
            Option(
                ('--cfg-path', 'cfg_path'),
                default=defaults.get('cfg_path', None),
                type=str,
                help='path of config file'
            ),
        ]
        return options

    @classmethod
    def register_workspace(cls, **kwargs) -> object:
        app_name = kwargs.pop('app_name', None)
        if app_name is not None:
            wcc = cls(app_name, **kwargs)
            wcc.start()
            return wcc
        else:
            return None

    def __init__(self,
            app_name: str,
            init_cfg: Optional[Union[str, Mapping[str, Any]]] = None,
            cfg_path: Optional[str] = None,
            **kwargs,
            ):
        r"""
        cfg_path: None -> no state saved; otherwise -> save state when terminated, read state when started if json file is not empty
        init_cfg: use when config read from cfg_path is empty
        """
        self.cfg_path = cfg_path
        self.cfg = {'workers': dict()}
        
        if init_cfg is not None:
            if isinstance(init_cfg, str):
                cfgs = parse_json_config(init_cfg)
                if cfgs is not None:
                    global_cfg, init_cfg =  cfgs
                else:
                    ValueError(
                        f'`init_cfg` should be a dict() in json.'
                    )
            elif isinstance(init_cfg, dict):
                global_cfg = init_cfg.pop('global', dict())
            else:
                TypeError(
                    f'Input argument `init_cfg` should be a path or a dict()'
                )

        if cfg_path is not None:
            cfgs = parse_json_config(cfg_path)
            if cfgs is not None:
                global_cfg, init_cfg = cfgs
            else:
                ValueError(
                    f'object in `cfg_path` should be a dict().'
                )
        pprint(init_cfg['workers'])

        self.global_cfg, self.init_cfg = global_cfg, init_cfg
        self.global_cfg.setdefault('workers', dict())
        self.init_cfg.setdefault('workers', dict())
        self.cfg_path = cfg_path
        self.app_name = app_name
        self.app = find_app(app_name)
        self._wpdict = OrderedDict()

    @property
    def nodes(self):
        return dict(self._wpdict)

    @property
    def hostnames(self):
        return list(self._wpdict.keys())

    def _get_nodes(self,
            nodes: Optional[Union[str, Iterable[str]]] = None
            ) -> List[str]:
        if nodes is None:
            return list(self._wpdict.keys())
        elif isinstance(nodes, str):
            nodes = [get_hostname(nodes)]
        else:
            nodes = [get_hostname(n) for n in nodes]
        return [n for n in self._wpdict.keys() if n in nodes]

    def _overload(self,
            nodes: Optional[Union[str, Iterable[str]]] = None,
            func: Optional[Callable] = None
            ):
        if isinstance(nodes, str):
            wp = self._wpdict.get(get_hostname(nodes))
            return func(wp) if wp and func else wp
        else:
            nodes = self._get_nodes(nodes)
            res = {
                n: func(wp) if func else wp
                for n, wp in self._wpdict.items()
                if n in nodes
            }
            return res

    def start_worker(self,
            node: str,
            run_config: Mapping[str, Any],
            wait_for_ready: bool = True
            ) -> str:
        node = get_hostname(node)
        if node in self._wpdict:
            print(f'hostname `{node}` duplicated')
            return 
        run_config['hostname'] = node
        wp = WorkerProcess(self.app_name, **run_config)
        wp.start()
        self._wpdict[node] = wp
        self.cfg['workers'][node] = run_config
        if wait_for_ready:
            if not wp.wait_for_ready():
                wp.shutdown()
                del self._wpdict[node]
                return None
        return node

    @property
    def control(self) -> Control:
        return self.app.control

    @property
    def inspect(self) -> Inspect:
        return self.app.control.inspect(self.hostnames)

    def info(self,
            nodes: Optional[Union[str, Iterable[str]]] = None
            ):
        return self._overload(nodes=nodes, func=lambda wp: wp.info())

    def stop_workers(self,
            nodes: Optional[Union[str, Iterable[str]]] = None,
            join: bool = True,
            timeout: Optional[int] = None
            ):
        nodes = self._get_nodes(nodes=nodes)
        for node in nodes:
            self._wpdict[node].shutdown(join=False)
            self.cfg['workers'].pop(node, None)
        if join:
            self.join(nodes, timeout=timeout)

    def join(self,
            nodes: Optional[Union[str, Iterable[str]]] = None,
            timeout: Optional[int] = None
            ):
        nodes = self._get_nodes(nodes=nodes)
        for node in nodes:
            self._wpdict[node].join(timeout=timeout)
            del self._wpdict[node]

    def start(self, eventloop: bool = False):
        for node, run_config in self.init_cfg['workers'].items():
            self.start_worker(node, run_config, wait_for_ready=False)
        if eventloop:
            try:
                pmain = ThreadingBranch(target=self._main_eventloop)
                pmain.start()
                pmain.join()
            except KeyboardInterrupt:
                self.terminate()

    def terminate(self, timeout: Optional[int] = None):
        # save config
        if self.cfg_path is not None:
            print('Save config')
            save_json_config(
                self.cfg_path,
                self.cfg,
                global_config=self.global_cfg
            )
        self.stop_workers(timeout=timeout)

    def _main_eventloop(self):
        try:
            while True: time.sleep(1000)
        except:
            pass
