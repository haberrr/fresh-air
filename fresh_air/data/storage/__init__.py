from typing import Type

from fresh_air._logging import get_logger
from fresh_air.data.storage.base import Resource
from fresh_air.data.storage.local import LocalResource
from fresh_air.data.storage.bigquery import BigQueryTable
from fresh_air.config import settings

logger = get_logger()


def resource_class_factory() -> Type[Resource]:
    storage_type = settings['storage.use_storage']
    logger.debug('Attempting to use logger type `%s`...', storage_type)

    if storage_type == 'local':
        return LocalResource
    elif storage_type == 'bigquery':
        return BigQueryTable
    else:
        raise NotImplementedError(f'Unknown storage_type `{storage_type}`.')
