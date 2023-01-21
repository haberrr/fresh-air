from typing import Type

from fresh_air.config import settings
from fresh_air.data.storage.file.avro import AvroFile
from fresh_air.data.storage.file.base import BaseFile
from fresh_air.data.storage.file.json import JsonFile


def file_class_factory() -> Type[BaseFile]:
    file_format = settings.get('storage.local.format')

    if file_format == 'json' or file_format is None:
        return JsonFile
    elif file_format == 'avro':
        return AvroFile
    else:
        raise NotImplementedError(f'Unknown file format `{file_format}`.')
