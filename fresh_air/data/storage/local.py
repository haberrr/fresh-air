import os
from typing import Any, Dict, List, Optional, Tuple

import prefect.task_runners

from fresh_air.data.storage.base import Resource
from fresh_air.config import settings
from fresh_air._logging import get_logger
from fresh_air.data.storage.file import BaseFile, file_class_factory

logger = get_logger(__name__)


class LocalResource(Resource):
    """
    Reflects data stored locally on the hard drive.

    Attributes:
        BASE_DIR: Local path to the directory with stored data. Defaults to the `./data`.
    """

    BASE_DIR: str = settings['storage.local.base_dir']
    DEFAULT_PROJ_DIR: str = '_default'
    FILE_CLASS: BaseFile = file_class_factory()

    def __init__(
            self,
            path: Tuple[str, ...] | str,
            project_id: Optional[str] = None,
            schema: Optional[Any] = None,
            **kwargs,
    ):
        """
        Instantiate resource object.

        Args:
            path: Unique identification path for the resource (under the `project_id` directory). When tuple of strings
                should represent location in the hierarchical directory structure with the last item in tuple serving
                as a name of the object. When string should represent the same location, with '.' separating leves of
                the hierarchy.
            project_id: (Optional) root directory of the path hierarchy.
            schema: Ignored.
        """
        self.path = path if isinstance(path, tuple) else tuple(path.split('.'))
        self.project_id = project_id or self.DEFAULT_PROJ_DIR

        self.schema = schema

        self._file = self.FILE_CLASS(self._dir_path, self.path[-1], schema=self.schema)

    @property
    def _dir_path(self) -> str:
        """Construct path to the directory with a file."""
        return os.path.expanduser(os.path.join(self.BASE_DIR, self.project_id, *self.path[:-1]))

    def write(
            self,
            data: List[Dict[str, Any]],
            append: bool = True,
            task_runner: Optional[prefect.task_runners.BaseTaskRunner] = None,
            **kwargs,
    ) -> None:
        """
        Write resource data to the file.

        Args:
            data: Data to write to the file.
            append: Whether to append or overwrite data.
            task_runner: Task runner for the current flow run. Used to acquire lock on the file being written.
            **kwargs: Added for compatibility, ignored.
        """
        logger.info('Saving %s', self.path)
        self._file.write(data, append=append, task_runner=task_runner)

    def read(self, **kwargs) -> List[Dict[str, Any]]:
        """Read resource data from file.

        Args:
            **kwargs: Added for compatibility, ignored.
        """
        logger.info('Reading %s', self.path)
        return list(self._file.read())
