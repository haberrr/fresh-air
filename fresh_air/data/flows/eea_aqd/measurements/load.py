from enum import Enum
from typing import Any, Dict, List, Optional, Iterable, Generator
from datetime import date

import prefect
from prefect.task_runners import BaseTaskRunner
from prefect_dask.task_runners import DaskTaskRunner
import typer
import requests
import pandas as pd

from fresh_air._logging import get_logger
from fresh_air.data.flows._utils import _read_query_from_neighbour
from fresh_air.data.flows.eea_aqd.measurements.table import meta_table, _columns_config, stg_table_factory, table
from fresh_air.data.flows.tasks import write_data
from fresh_air.data.storage import Resource, BigQueryTable


class TimeCoverage(str, Enum):
    Last7days = 'Last7days'
    Year = 'Year'


def _chunked(iterable: Iterable, chunk_size: int) -> Generator[list, None, None]:
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []

    if len(chunk) > 0:
        yield chunk


@prefect.task(
    name='Get a list of EEA AQD measurement report urls',
    retries=3,
    retry_delay_seconds=15,
)
def get_eea_aqd_measurement_report_urls(
        country_code: Optional[str] = None,
        pollutant_code: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        station: Optional[str] = None,
        time_coverage: TimeCoverage = 'Last7days',
) -> List[str]:
    """
    Request report URLs containing measurements information from EEA AQD database.

    Args:
        country_code: Two character country code of the air quality station to retrieve measurements from.
        pollutant_code: Code of the pollutant.
            Vocabulary with the possible values for the codes: https://dd.eionet.europa.eu/vocabulary/aq/pollutant/view.
        year_from: Request data starting from this year.
        year_to: Request data up to this year inclusive.
        station: Station code to request data from.
        time_coverage: Whether to request data from the past 7 days of for the whole year.

    Returns:
        List of report URLs filtered by the args.
    """
    logger = get_logger(__name__)
    logger.info('Requesting report URL list...')

    URL = 'https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw'
    response = requests.get(
        URL,
        params={
            'CountryCode': country_code or '',
            'CityName': '',
            'Pollutant': pollutant_code or '',
            'Year_from': year_from or date.today().year,
            'Year_to': year_to or date.today().year,
            'Station': station or '',
            'Samplingpoint': '',
            'Source': 'All',
            'Output': 'TEXT',
            'UpdateDate': '',
            'TimeCoverage': time_coverage,
        },
    )

    response.raise_for_status()
    report_urls = response.content.decode('utf-8-sig').split()

    logger.info('Total number of reports: %s', len(report_urls))

    return report_urls


@prefect.task(
    name='Get EEA AQD measurement report data',
    retries=3,
    retry_delay_seconds=15,
)
def get_eea_aqd_measurement_report_data(report_url: str) -> List[Dict[str, Any]]:
    """
    Download and preprocess report data.

    This function will select a subset of the columns and perform basic preprocessing for a part of them (like
    extracting pollutant code from the URL to the vocabulary or converting datetime string to float timestamp).

    Args:
        report_url: URL to download report from. Normally this should be one of the URLs output
            by the `get_eea_aqd_measurement_report_urls()`.

    Returns:
        Preprocessed report data.
    """
    logger = get_logger(__name__)
    logger.info('Downloading report from URL: %s', report_url)

    report = pd.read_csv(
        report_url
    )[[
        col['name'] for col in _columns_config if 'name' in col
    ]].assign(
        **{
            col['name']: col['preprocess'](col['name']) for col in _columns_config if 'preprocess' in col
        },
        _report_url=report_url,
    ).rename(columns={
        col['name']: col['field'].name for col in _columns_config if 'name' in col
    }).astype({
        col['field'].name: col['field'].field_type for col in _columns_config if col.get('convert', False)
    }).replace({
        float('nan'): None
    }).to_dict(
        orient='records',
    )

    logger.info('Report (URL: %s) downloaded, record count: %s', report_url, len(report))

    return report


@prefect.task(
    name='Get EEA AQD measurement report data - batched',
    retries=3,
    retry_delay_seconds=15,
    timeout_seconds=180,
)
def get_eea_aqd_measurement_report_batch_data(
        report_urls: List[str],
        table_: Resource,
        append: bool = True,
        task_runner: Optional[BaseTaskRunner] = None,
) -> None:
    """
    Download a batch of reports from given `report_urls` and write them to a `table_`.

    Notes:
        This task performs two jobs: downloading and preprocessing data, and saving data to a table. This is a forced
        measure due to the memory leak in when using Dask task runner to run tasks that return report data. Structuring
        tasks this way avoids this issue.

    Args:
        report_urls: List of report URLs for the batch.
        table_: Resource used to store reports' data.
        append: Whether to append or to overwrite existing data.
        task_runner: Optional task runner used to run this task. In case of the LocalResource storage it is used to
            create Lock on the file to avoid writing to a file simultaneously from different workers/threads.
    """
    logger = get_logger(__name__)

    reports = []
    for i, report_url in enumerate(report_urls):
        reports.extend(get_eea_aqd_measurement_report_data.fn(report_url))
        logger.info('Report %s/%s downloaded', i + 1, len(report_urls))

    logger.info('Total record count for %s reports: %s', len(report_urls), len(reports))
    write_data.fn(
        reports,
        table_,
        append,
        task_runner,
    )


@prefect.flow(
    name='Save EEA AQD measurements (staging)',
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            'n_workers': 1,
            'threads_per_worker': 8,
            'memory_spill_fraction': 1.0,
            'memory_pause_fraction': 1.0,
        },
    ),
)
def eea_adq_measurements_stg(
        country_code: Optional[str] = None,
        pollutant_code: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        station: Optional[str] = None,
        time_coverage: TimeCoverage = 'Last7days',
        batch_size: int = 10,
) -> Resource:
    """
    Retrieve report URLs, download, preprocess and save reports to a temporary table.

    Args:
        country_code: Two character country code of the air quality station to retrieve measurements from.
        pollutant_code: Code of the pollutant.
            Vocabulary with the possible values for the codes: https://dd.eionet.europa.eu/vocabulary/aq/pollutant/view.
        year_from: Request data starting from this year.
        year_to: Request data up to this year inclusive.
        station: Station code to request data from.
        time_coverage: Whether to request data from the past 7 days of for the whole year.
        batch_size: Size of the report URLs batch to assign to a single Dask worker/thread.

    Returns:
        Instance of the temporary table resource where the data was saved.
    """
    flow_context = prefect.context.get_run_context()
    stg_table = stg_table_factory(flow_context.flow_run.flow_id)

    if time_coverage == TimeCoverage.Year:
        timeout = 30 * batch_size
    else:
        timeout = 10 * batch_size

    download_reports = get_eea_aqd_measurement_report_batch_data.with_options(timeout_seconds=timeout)
    report_urls = get_eea_aqd_measurement_report_urls.submit(
        country_code,
        pollutant_code,
        year_from,
        year_to,
        station,
        time_coverage
    ).result()

    write_data.submit(
        [{'report_url': report_url} for report_url in report_urls],
        meta_table,
        task_runner=flow_context.task_runner,
    )

    for batch in _chunked(report_urls, batch_size):
        download_reports.submit(
            batch,
            stg_table,
            append=True,
            task_runner=flow_context.task_runner,
        )

    return stg_table


@prefect.flow(
    name='Save EEA AQD measurements',
)
def eea_aqd_measurements_core(
        country_code: Optional[str] = None,
        pollutant_code: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        station: Optional[str] = None,
        time_coverage: TimeCoverage = 'Last7days',
        batch_size: int = 10,
) -> None:
    """
    Retrieve report URLs, download, preprocess, save reports to a temporary table
    then update core storage table with fresh report data.

    Args:
        country_code: Two character country code of the air quality station to retrieve measurements from.
        pollutant_code: Code of the pollutant.
            Vocabulary with the possible values for the codes: https://dd.eionet.europa.eu/vocabulary/aq/pollutant/view.
        year_from: Request data starting from this year.
        year_to: Request data up to this year inclusive.
        station: Station code to request data from.
        time_coverage: Whether to request data from the past 7 days of for the whole year.
        batch_size: Size of the report URLs batch to assign to a single Dask worker/thread.
    """
    logger = get_logger()

    stg_table = eea_adq_measurements_stg(
        country_code=country_code,
        pollutant_code=pollutant_code,
        year_from=year_from,
        year_to=year_to,
        station=station,
        time_coverage=time_coverage,
        batch_size=batch_size,
    )

    if not isinstance(table, BigQueryTable) or not isinstance(stg_table, BigQueryTable):
        logger.warning('Target resource is not BigQueryTable. Flow won\'t update data the target table.')
        return

    query = _read_query_from_neighbour(__file__, 'merge.sql')
    table.run_query(query, source=stg_table.full_table_name, wait_for_result=True)


if __name__ == '__main__':
    typer.run(eea_aqd_measurements_core)
