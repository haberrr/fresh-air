from typing import Optional

from prefect.task_runners import BaseTaskRunner
from prefect_dask import DaskTaskRunner, get_dask_client
import dask.distributed


class Lock:
    def __init__(self, full_file_path: str, task_runner: Optional[BaseTaskRunner] = None):
        self.task_runner = task_runner
        self.full_file_path = full_file_path

        if isinstance(self.task_runner, DaskTaskRunner):
            self._lock = dask.distributed.Lock(self.full_file_path)

    def acquire(self) -> None:
        if hasattr(self, '_lock'):
            self._lock.acquire()

    def release(self) -> None:
        if hasattr(self, '_lock'):
            self._lock.release()

    def __enter__(self) -> 'Lock':
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
