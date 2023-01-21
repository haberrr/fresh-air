import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

from prefect.task_runners import BaseTaskRunner

from fresh_air._logging import get_logger
from fresh_air.data.storage.base import SchemaField

logger = get_logger(__name__)

_meta_fields = [
    SchemaField(
        name='_etl_timestamp',
        field_type='timestamp',
        description='Timestamp when the record was saved.',
    ),
]


def _add_meta(records: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    ts = datetime.now().timestamp()
    return ({**record, '_etl_timestamp': ts} for record in records)


class BaseFile(ABC):
    """Base class for local filesystem read/write operations."""

    file_path: str
    file_name: str
    _extension: str

    @abstractmethod
    def __init__(self, file_path, file_name, **kwargs):
        pass

    @abstractmethod
    def write(
            self,
            records: Iterable[Dict[str, Any]],
            append: bool = True,
            task_runner: Optional[BaseTaskRunner] = None,
    ):
        pass

    @abstractmethod
    def read(self) -> Iterable[Dict[str, Any]]:
        pass

    def _ensure_path_exists(self) -> None:
        if not os.path.exists(self.file_path):
            logger.info('Storage folder does not exist, creating...')
            os.makedirs(self.file_path)

    @property
    def _full_path(self) -> str:
        return os.path.expanduser(os.path.join(
            self.file_path,
            f'{self.file_name}.{self._extension}',
        ))
