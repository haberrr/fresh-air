import os
from functools import cache
from itertools import chain
from typing import List, Dict, Any, Optional, Tuple, Union

from google.cloud import bigquery

from fresh_air._logging import get_logger
from fresh_air.data.storage.base import Resource, SchemaField
from fresh_air.config import settings
from fresh_air.data.storage.meta import add_meta, meta_fields

logger = get_logger()


def get_client():
    """Provides single BigQuery client."""
    credentials_json = settings.get('credentials.etl_service_account_json')
    if credentials_json is not None and os.path.exists(credentials_json):
        logger.debug('Using provided service account credentials for BigQuery client')
        return bigquery.Client.from_service_account_json(
            os.path.expanduser(credentials_json)
        )
    else:
        logger.debug('Using ADC for BigQuery client')
        return bigquery.Client()


BIGQUERY_TYPE = {
    'string': bigquery.SqlTypeNames.STRING,
    'int': bigquery.SqlTypeNames.INTEGER,
    'float': bigquery.SqlTypeNames.FLOAT,
    'bool': bigquery.SqlTypeNames.BOOLEAN,
    'timestamp': bigquery.SqlTypeNames.TIMESTAMP,
    'datetime': bigquery.SqlTypeNames.DATETIME,
    'date': bigquery.SqlTypeNames.DATE,
}


class BigQueryTable(Resource):
    """Reflects data stored in the BigQuery table."""

    def __init__(
            self,
            path: Tuple[str, str] | str,
            schema: List[SchemaField],
            project_id: Optional[str] = None,
            clustering_fields: Optional[List[str]] = None,
            partition_field: Optional[str] = None,
            partition_scale: Optional[Union[bigquery.TimePartitioningType, str]] = None,
            **kwargs,
    ):
        """
        Instantiate resource object.

        Args:
            path: Unique identification path for the resource. When tuple of strings, the first element should be of
                the form (project_id, dataset_id, table_name). When string, should represent the same location,
                with '.' separating leves of the hierarchy.
            schema: Table schema as a list of SchemaField objects. Field type should be one of the supported
                BigQuery types.
            project_id: BigQuery project ID to save data to.
            partition_field: Field to use for table partitioning (must be of time TIMESTAMP, DATE or DATETIME).
            partition_scale: Time scale to use when partitioning a table (hour, day, month or year).
        """
        if isinstance(path, str):
            path = tuple(path.split('.'))

        self.dataset_id, self.table_id = path
        self.project_id = project_id or settings.get('storage.bigquery.project_id')

        self.schema = schema
        self.clustering_fields = clustering_fields

        if (partition_scale is None) ^ (partition_field is None):
            raise ValueError('Either both `partition_scale` and `partition_field` should be defined or none.')

        if isinstance(partition_scale, str):
            partition_scale = getattr(bigquery.TimePartitioningType, partition_scale.upper())

        self.partition_field = partition_field
        self.partition_scale = partition_scale

    def write(self, data: List[Dict[str, Any]], append: bool = True, **kwargs) -> None:
        """
        Write resource data to the BigQuery table.

        Args:
            data: Data to write to the file.
            append: Whether to append or overwrite data.
            **kwargs: For compatibility purposes, ignored.
        """
        self._ensure_table_exists()

        if append:
            write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        else:
            write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

        bq_client = get_client()
        job = bq_client.load_table_from_json(
            json_rows=add_meta(data),
            destination=self._table,
            job_config=bigquery.LoadJobConfig(
                schema=self._table.schema,
                write_disposition=write_disposition,
                schema_update_options=bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            )
        )
        job.result()

    def read(self, **kwargs) -> List[Dict[str, Any]]:
        bq_client = get_client()
        return [dict(row) for row in bq_client.list_rows(self._table)]

    @property
    @cache
    def _table(self) -> bigquery.Table:
        schema = []
        for field in chain(self.schema, meta_fields):
            if isinstance(field.field_type, type):
                field_type = BIGQUERY_TYPE.get(field.field_type.__name__, 'STRING')
            elif isinstance(field.field_type, str):
                field_type = BIGQUERY_TYPE.get(field.field_type, 'STRING')
            else:
                raise TypeError(f'Unknown type ({type(field.field_type)}) for `field_type` of {field}')

            schema.append(
                bigquery.SchemaField(
                    name=field.name,
                    field_type=field_type,
                    description=field.description,
                    mode=field.mode,
                )
            )

        table = bigquery.Table(
            f'{self.project_id}.{self.dataset_id}.{self.table_id}',  # noqa
            schema=schema,
        )
        table.clustering_fields = self.clustering_fields

        if self.partition_field is not None:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=self.partition_scale,
                field=self.partition_field,  # noqa
            )

        return table

    def _ensure_table_exists(self) -> None:
        bq_client = get_client()
        bq_client.create_dataset(
            bigquery.Dataset.from_string(f'{self.project_id}.{self.dataset_id}'),
            exists_ok=True,
        )
        bq_client.create_table(self._table, exists_ok=True)
