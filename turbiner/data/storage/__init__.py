from typing import Type

from turbiner.data.storage.base import Resource
from turbiner.data.storage.local import LocalResource
from turbiner.config import settings


def resource_class_factory() -> Type[Resource]:
    storage_type = settings['storage.use_storage']
    if storage_type == 'local':
        return LocalResource
    else:
        raise NotImplementedError(f'Unknown storage_type `{storage_type}`.')
