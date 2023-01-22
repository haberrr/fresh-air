from enum import Enum
from typing import Any, Dict, List, Optional, Iterable, Generator
from datetime import date

import prefect
from prefect.task_runners import BaseTaskRunner
from prefect_dask.task_runners import DaskTaskRunner
import dask
import typer
import requests
import pandas as pd

from fresh_air._logging import get_logger
from fresh_air.data.flows.eea_aqd.measurements.table import table, _columns_config
from fresh_air.data.flows.tasks import write_data
from fresh_air.data.storage import Resource


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
    logger = get_logger(__name__)
    logger.info('Downloading report from URL: %s', report_url)

    report = pd.read_csv(
        report_url
    )[[
        col['name'] for col in _columns_config
    ]].assign(**{
        col['name']: col['preprocess'](col['name']) for col in _columns_config if 'preprocess' in col
    }).rename(columns={
        col['name']: col['field'].name for col in _columns_config
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
    name='Load EEA AQD measurements',
    task_runner=DaskTaskRunner(cluster_kwargs={
        'n_workers': 2,
        'threads_per_worker': 2,
    }),
)
def load_eeq_aqd_measurements(
        country_code: Optional[str] = None,
        pollutant_code: Optional[int] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        station: Optional[str] = None,
        time_coverage: TimeCoverage = 'Last7days',
        batch_size: int = 10,
) -> None:
    flow_context = prefect.context.get_run_context()

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

    with dask.config.set({'distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_': 0}):
        for batch in _chunked(report_urls, batch_size):
            download_reports.submit(
                batch,
                table,
                append=True,
                task_runner=flow_context.task_runner,
            )


if __name__ == '__main__':
    typer.run(load_eeq_aqd_measurements)
