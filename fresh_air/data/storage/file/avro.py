from typing import Iterable, Dict, Any, List, Optional

from fastavro import writer, reader, parse_schema
from prefect.task_runners import BaseTaskRunner

from fresh_air.data.storage.base import SchemaField
from fresh_air.data.storage.file._lock import Lock
from fresh_air.data.storage.file.base import BaseFile
from fresh_air.data.storage.meta import meta_fields, add_meta

AVRO_TYPE = {
    'str': 'string',
    'datetime': 'string',
    'date': 'string',
    'timestamp': 'float',
    'float': 'float',
    'int': 'int',
    'bool': 'boolean',
    'boolean': 'boolean',
}


def _convert_field_to_avro(field: SchemaField) -> Dict[str, str]:
    if isinstance(field.field_type, type):
        field_type = AVRO_TYPE.get(field.field_type.__name__, 'string')
    elif isinstance(field.field_type, str):
        field_type = AVRO_TYPE.get(field.field_type.lower(), 'string')
    else:
        raise TypeError(f'Unknown type ({type(field.field_type)}) for `field_type` of {field}')

    avro_field = {
        'name': field.name,
        'type': [field_type, 'null'],
    }

    if field.description:
        avro_field['doc'] = field.description

    if field.default:
        avro_field['default'] = field.default

    return avro_field


class AvroFile(BaseFile):
    """Class implementing interface for interaction with Avro file."""

    _extension: str = 'avro'

    def __init__(
            self,
            file_path: str,
            file_name: str,
            schema: List[SchemaField],
            codec: Optional[str] = 'deflate',
            **kwargs,
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.schema = schema
        self.codec = codec

        self._parsed_schema = parse_schema({
            'name': self.file_name,
            'type': 'record',
            'fields': [_convert_field_to_avro(field) for field in schema + meta_fields]
        })

    def write(
            self,
            records: Iterable[Dict[str, Any]],
            append: bool = True,
            task_runner: Optional[BaseTaskRunner] = None,
            **kwargs,
    ) -> None:
        """
        Write lines to an Avro file.

        Args:
            records: Iterable of records.
            append: Whether to append to or overwrite existing file.
            task_runner: When run within a task, it is used to determine how to acquire lock on the file.
        """
        self._ensure_path_exists()

        with Lock(self._full_path, task_runner):
            with open(self._full_path, ('a+' if append else 'w') + 'b') as f:
                writer(f, self._parsed_schema, add_meta(records), codec=self.codec)

    def read(self) -> Iterable[Dict[str, Any]]:
        """
        Read from file with json lines.

        Returns:
            Iterator over records in the file.
        """

        with open(self._full_path, 'rb') as f:
            yield from reader(f)
