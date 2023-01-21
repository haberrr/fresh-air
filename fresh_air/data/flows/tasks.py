from typing import List, Dict, Any

import prefect

from fresh_air.data.storage import Resource
from fresh_air.data.storage.bigquery import BigQueryTable, get_client


@prefect.task(name='Write table data to BigQuery', tags=['bigquery'])
def write_to_bigquery(data: List[Dict[str, Any]], table: BigQueryTable, append: bool = True) -> None:
    table.write(data, append=append)


@prefect.task(name='Read table data from BigQuery', tags=['bigquery'])
def read_from_bigquery(table: BigQueryTable) -> List[Dict[str, Any]]:
    return table.read()


@prefect.task(name='Get query results from BigQuery', tags=['bigquery'])
def query_bigquery(query: str) -> List[Dict[str, Any]]:
    client = get_client()
    job = client.query(query)
    return [dict(row) for row in job.result()]


@prefect.task(name='Write data to storage', tags=['storage', 'write'])
def write_data(data: List[Dict[str, Any]], table: Resource, append: bool = True) -> None:
    table.write(data, append=append)
