import json
from datetime import datetime
from typing import Iterable, Dict, Any

from fresh_air.data.storage.file.base import BaseFile, _add_meta


class JsonFile(BaseFile):
    """Class implementing interface for interaction with JSON-lines file."""

    extension: str = 'jsonl'

    def __init__(self, file_path: str, file_name: str, **kwargs):
        self.file_path = file_path
        self.file_name = file_name

    def write(self, records: Iterable[Dict[str, Any]], append: bool = True) -> None:
        """
        Write json lines to a file.

        Args:
            records: Iterable of records.
            append: Whether to append to or overwrite existing file.
        """
        self._ensure_path_exists()

        with open(self._full_path, ('a' if append else 'w') + 't') as f:
            for record in _add_meta(records):
                f.write(json.dumps(record) + '\n')

    def read(self) -> Iterable[Dict[str, Any]]:
        """
        Read from file with json lines.

        Returns:
            Iterator over records in the file.
        """

        with open(self._full_path, 'rt') as f:
            return (json.loads(line) for line in f)
