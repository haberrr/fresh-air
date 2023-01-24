import json
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

from prefect.task_runners import BaseTaskRunner

from fresh_air.data.storage.file._lock import Lock
from fresh_air.data.storage.file.base import BaseFile
from fresh_air.data.storage.meta import add_meta


class JsonFile(BaseFile):
    """Class implementing interface for interaction with JSON-lines file."""

    _extension: str = 'jsonl'

    def __init__(self, file_path: str, file_name: str, **kwargs):
        self.file_path = file_path
        self.file_name = file_name

    def write(
            self,
            records: Iterable[Dict[str, Any]],
            append: bool = True,
            task_runner: Optional[BaseTaskRunner] = None,
            **kwargs,
    ) -> None:
        """
        Write json lines to a file.

        Args:
            records: Iterable of records.
            append: Whether to append to or overwrite existing file.
            task_runner: When run within a task, it is used to determine how to acquire lock on the file.
        """
        self._ensure_path_exists()

        with Lock(self._full_path, task_runner):
            with open(self._full_path, ('a' if append else 'w') + 't') as f:
                for record in add_meta(records):
                    f.write(json.dumps(record) + '\n')

    def read(self) -> Iterable[Dict[str, Any]]:
        """
        Read from file with json lines.

        Returns:
            Iterator over records in the file.
        """

        with open(self._full_path, 'rt') as f:
            return (json.loads(line) for line in f)
