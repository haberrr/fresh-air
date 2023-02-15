from typing import List, Dict, Any, Optional, Union

import prefect
from google.cloud import bigquery
from prefect.task_runners import BaseTaskRunner

from fresh_air.data.storage import Resource
from fresh_air.data.storage.bigquery import BigQueryTable, get_client


@prefect.task(name='Write data to storage', tags=['storage', 'write'], retries=3, timeout_seconds=60)
def write_data(
        data: List[Dict[str, Any]],
        table: Resource,
        append: bool = True,
        task_runner: Optional[BaseTaskRunner] = None,
) -> None:
    table.write(data, append=append, task_runner=task_runner)


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


@prefect.task(name='BigQuery snapshot', tags=['bigquery'])
def bigquery_snapshot(
        source: Union[BigQueryTable, str],
        target: BigQueryTable,
        columns: Union[Dict[str, str], List[str]] = None,
        wait_for_result: bool = True,
) -> bigquery.QueryJob:
    """
    BigQuery task overwrites data in the target table with data from source.

    Args:
        source: Table containing source data or query providing source data.
        target: Table to which to append data.
        columns: Either list of column names or dict containing mapping from source column names to target column names.
        wait_for_result: Whether to wait for the query job to finish before returning.

    Returns:
        BigQuery QueryJob instance.

    Raises:
        TypeError when source type is different from string or BigQueryTable.
    """
    if isinstance(columns, dict):
        columns = ', '.join(f'{s} AS {t}' for s, t in columns.items())
    elif isinstance(columns, list):
        columns = ', '.join(columns)
    else:
        columns = '*'

    source_definition = _make_source_definition(source)

    query = f'''
    BEGIN 
        TRUNCATE TABLE `{target.full_table_name};`
    
        INSERT INTO `{target.full_table_name}` AS
        SELECT {columns}
        FROM {source_definition};
    END;
    '''

    return target.run_query(query, wait_for_result)


@prefect.task(name='BigQuery append', tags=['bigquery'])
def bigquery_append(
        source: Union[BigQueryTable, str],
        target: BigQueryTable,
        columns: Union[Dict[str, str], List[str]] = None,
        wait_for_result: bool = True,
) -> bigquery.QueryJob:
    """
    BigQuery task that append data from source table to the target table.

    Args:
        source: Table or query containing source data.
        target: Table to which to append data.
        columns: Either list of column names or dict containing mapping from source column names to target column names.
        wait_for_result: Whether to wait for the query job to finish before returning.

    Returns:
        BigQuery QueryJob instance.
    """
    if isinstance(columns, dict):
        columns = ', '.join(f'{s} AS {t}' for s, t in columns.items())
    elif isinstance(columns, list):
        columns = ', '.join(columns)
    else:
        columns = '*'

    source_definition = _make_source_definition(source)

    query = f'''
    INSERT INTO `{target.full_table_name}` AS
    SELECT {columns}
    FROM {source_definition}
    '''

    return target.run_query(query, wait_for_result)


@prefect.task(name='BigQuery merge', tags=['bigquery'])
def bigquery_merge(
        source: Union[BigQueryTable, str],
        target: BigQueryTable,
        merge_keys: List[str],
        condition_keys: List[str] = None,
        wait_for_result: bool = True,
) -> bigquery.QueryJob:
    """
    BigQuery task that upserts data from `source` table into the `target` table based
    on `merge_keys` and `condition_keys`.

    Args:
        source: Table or query containing source data.
        target: Table to which to upsert data.
        merge_keys: List of columns comprising primary key for the tables. Based on these keys the tables
         will be joined together.
        condition_keys: List of columns to check when updating the row data. If values of these columns in source and
         target tables do not coincide, the row will be updated; otherwise, it will be left unchanged.
        wait_for_result: Whether to wait for the query job to finish before returning.

    Returns:
        BigQuery QueryJob instance.
    """
    target_columns = [field.name for field in target._table.schema]
    merge_clause = ' AND '.join(f'T.{key} = S.{key}' for key in merge_keys)

    source_definition = _make_source_definition(source)

    if condition_keys is not None:
        condition_clause = 'AND ({})'.format(
            ' OR '.join(f'T.{key} <> S.{key}' for key in condition_keys)
        )
    else:
        condition_clause = ''

    matched_clause = 'UPDATE SET {}'.format(
        ', '.join(f'{col} = S.{col}' for col in target_columns)
    )

    not_matched_clause = 'INSERT ({}) VALUES ({})'.format(
        ', '.join(target_columns),
        ', '.join(f'S.{col}' for col in target_columns),
    )

    query = f'''
    MERGE INTO `{target.full_table_name}` AS T
    USING {source_definition} AS S
        ON  {merge_clause}
    WHEN MATCHED {condition_clause} THEN {matched_clause}
    WHEN NOT MATCHED THEN {not_matched_clause}
    '''

    return target.run_query(query, wait_for_result)


def _make_source_definition(source: Union[BigQueryTable, str]) -> str:
    if isinstance(source, BigQueryTable):
        return f'`{source.full_table_name}`'
    elif isinstance(source, str):
        return f'({source})'
    else:
        raise TypeError(f'Unknown source type {type(source)}.')
