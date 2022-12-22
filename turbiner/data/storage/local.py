import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from turbiner.data.storage.base import Resource
from turbiner.config import settings
from turbiner._logging import get_logger

logger = get_logger(__name__)


class LocalResource(Resource):
    """
    Reflects data stored locally on the hard drive.

    Attributes:
        BASE_DIR: Local path to the directory with stored data. Defaults to the `./data`.
    """

    BASE_DIR: str = settings['storage.local.base_dir']

    def __init__(
            self,
            path: Tuple[str, ...] | str,
            data: Optional[List[Dict[str, Any]]] = None,
            schema: Optional[Any] = None,
    ):
        """
        Instantiate resource object.

        Args:
            path: Unique identification path for the resource. When tuple of strings should represent location in
                the hierarchical directory structure with the last item in tuple serving as a name of the object.
                When string should represent the same location, with '.' separating leves of the hierarchy.
            data: Optional data, to be written into the resource location as defined by `path`.
            schema: Ignored.
        """
        self.path = path if isinstance(path, tuple) else tuple(path.split('.'))
        self.data = data
        self.schema = schema

    @property
    def _full_path(self) -> str:
        """Construct full path to the file."""
        return os.path.expanduser(os.path.join(self.BASE_DIR, *self.path[:-1], f'{self.path[-1]}.jsonl'))

    @property
    def _dir_path(self) -> str:
        """Construct path to the directory with a file."""
        return os.path.expanduser(os.path.join(self.BASE_DIR, *self.path[:-1]))

    def _ensure_path_exists(self) -> None:
        if not os.path.exists(self._dir_path):
            logger.info('Storage folder does not exist, creating...')
            os.makedirs(self._dir_path)

    def write(self, data: Optional[List[Dict[str, Any]]] = None, append: bool = True, **kwargs) -> None:
        """
        Write resource data to the file.

        Args:
            data: Data to write to the file. When provided replaces the data from the constructor.
            append: Whether to append or overwrite data.
            **kwargs: Added for compatibility, ignored.
        """
        if data is not None:
            self.data = data

        if self.data is None:
            raise ValueError('No data provided. Please provide data upon initialization or write call.')

        self._ensure_path_exists()
        logger.info('Saving to "%s"', self._full_path)

        with open(self._full_path, ('a' if append else 'w') + 't') as f:
            for item in self.data:
                json.dump({
                    '_etl_timestamp': datetime.now().timestamp(),
                    'data': item,
                }, f)
                f.write('\n')

    def read(self, **kwargs) -> List[Dict[str, Any]]:
        """Read resource data from file.

        Args:
            **kwargs: Added for compatibility, ignored.
        """
        logger.info('Reading from "%s"', self._full_path)

        data = []
        with open(self._full_path, 'rt') as f:
            for line in f:
                data.append(
                    json.loads(line).get('data'),
                )

        self.data = data
        return self.data
