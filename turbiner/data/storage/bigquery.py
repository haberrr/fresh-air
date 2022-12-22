import os
from datetime import datetime
from functools import cache
from typing import List, Dict, Any, Optional, Tuple

from google.cloud import bigquery

from turbiner.data.storage.base import Resource, SchemaField
from turbiner.config import settings


@cache
def get_client():
    """Provides single BigQuery client."""
    credentials_json = settings.get('credentials.etl_service_account_json')
    if credentials_json is not None:
        return bigquery.Client.from_service_account_json(
            os.path.expanduser(credentials_json)
        )
    else:
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
            path: Tuple[Optional[str], str, str] | str,
            schema: List[SchemaField],
            data: Optional[List[Dict[str, Any]]] = None,
            clustering_fields: Optional[List[str]] = None,
    ):
        """
        Instantiate resource object.

        Args:
            path: Unique identification path for the resource. When tuple of strings, the first element should be of
                the form (project_id, dataset_id, table_name). When string, should represent the same location,
                with '.' separating leves of the hierarchy.
            data: Optional data, to be written into the resource location as defined by `path`.
            schema: Table schema of the form:
                    ```[
                        {'name': field_name_1, 'type': field_type_1, ...},
                        {'name': field_name_2, 'type': field_type_2, ...},
                        ...
                    ]```,
                Field type should be one of the supported BigQuery types.
        """
        if isinstance(path, str):
            path = tuple(path.split('.'))

        self.project_id, self.dataset_id, self.table_id = path
        if self.project_id is None:
            self.project_id = settings.get('storage.bigquery.project_id')

        self.schema = schema
        self.data = data
        self.clustering_fields = clustering_fields

    def write(
            self,
            data: Optional[List[Dict[str, Any]]] = None,
            append: bool = True,
            **kwargs,
    ) -> None:
        """
        Write resource data to the BigQuery table.

        Args:
            data: Data to write to the file. When provided replaces the data from the constructor.
            append: Whether to append or overwrite data.
            **kwargs: For compatibility purposes, ignored.
        """
        if data is not None:
            self.data = data

        if self.data is None:
            raise ValueError('No data provided. Please provide data upon initialization or write call.')

        self._ensure_table_exists()

        if append:
            write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        else:
            write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

        bq_client = get_client()
        etl_timestamp = datetime.now().timestamp()
        job = bq_client.load_table_from_json(
            json_rows=({**row, '_etl_timestamp': etl_timestamp} for row in self.data),
            destination=self._table,
            job_config=bigquery.LoadJobConfig(
                schema=self._table.schema,
                write_disposition=write_disposition,
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
        for field in self.schema:
            schema.append(
                bigquery.SchemaField(
                    name=field.name,
                    field_type=BIGQUERY_TYPE.get(field.field_type, 'STRING'),
                    description=field.description,
                    mode=field.mode,
                )
            )

        schema.append(
            bigquery.SchemaField(
                name='_etl_timestamp',
                field_type='TIMESTAMP',
                description='Technical field, timestamp of the ETL job.',
            )
        )

        table = bigquery.Table(
            f'{self.project_id}.{self.dataset_id}.{self.table_id}',
            schema=schema,
        )
        table.clustering_fields = self.clustering_fields
        return table

    def _ensure_table_exists(self) -> None:
        bq_client = get_client()
        bq_client.create_table(self._table, exists_ok=True)
