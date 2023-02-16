from typing import Optional, List
from datetime import datetime

import prefect
import pendulum
import typer
from google.cloud import bigquery

from fresh_air._logging import get_logger
from fresh_air.data.flows.eea_aqd.metadata.table import table as eea_aqd_metadata_table
from fresh_air.data.flows.gfs.default_points.table import table as default_gfs_points_table
from fresh_air.data.flows.gfs.forecasts.table import table
from fresh_air.data.flows.tasks import bigquery_merge
from fresh_air.data.storage import BigQueryTable

GFS_PUBLIC_DATA_TABLE = 'bigquery-public-data.noaa_global_forecast_system.NOAA_GFS0P25'
POINT_TEMPLATE = 'ST_GEOGPOINT({longitude}, {latitude})'


@prefect.task(
    name='Get default GFS query points',
)
def get_default_gfs_points() -> List[str]:
    """
    Get default GFS query points as defined by the respective flow.

    Returns:
        List of points as specified by the `POINT_TEMPLATE`.
    """
    return [POINT_TEMPLATE.format_map(row) for row in default_gfs_points_table.read()]


@prefect.task(
    name='Get GFS query points from air quality stations',
)
def get_gfs_point_from_stations(air_quality_stations: List[str]) -> List[str]:
    if not isinstance(eea_aqd_metadata_table, BigQueryTable):
        raise NotImplementedError('Only BigQuery storage is supported for the GFS data extraction.')

    query = f'''
    SELECT DISTINCT
        longitude, 
        latitude
    FROM (
        SELECT
            air_quality_station,
            ROUND(AVG(longitude) * 4) / 4 AS longitude,
            ROUND(AVG(latitude) * 4) / 4  AS latitude
        FROM `{{table}}`
        WHERE air_quality_station IN UNNEST(@air_quality_station_list)
        GROUP BY 1
    )
    '''

    # noinspection PyTypeChecker
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter('air_quality_station_list', 'STRING', air_quality_stations),
        ],
    )

    job = eea_aqd_metadata_table.run_query(query, wait_for_result=True, job_config=job_config)
    return [POINT_TEMPLATE.format_map(row) for row in job.result()]


@prefect.flow(
    name='Save GFS forecasts',
)
def save_gfs_forecasts(
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        air_quality_stations: Optional[List[str]] = None,
        forecast_horizon_h: int = 48,
) -> None:
    """
    Get GFS forecast data from BigQuery public dataset, filter by location and save to a project table.

    By default saves yesterday's forecast data for an active subset of air quality stations.

    Args:
        start_date: Start date from which to save weather forecast. Yesterday by default.
        end_date: End date (exclusive) up to which to save weather forecast. Today by default, hence saving
         only yesterday's data.
        latitude: Optional latitude for which to save weather forecast data.
        longitude: Optional longitude for which to save weather forecast data.
        air_quality_stations: List (or one) of air quality stations to save weather forecast data for.
        forecast_horizon_h: Forecast horizon in hours to extract from source data.
    """
    logger = get_logger(__name__)

    start_date = (start_date or pendulum.today(tz='UTC').subtract(days=1)).date()
    end_date = (end_date or pendulum.today(tz='UTC')).date()

    if (latitude is None) ^ (longitude is None):
        raise ValueError('Either both latitude and longitude should be provided or none.')
    elif (latitude is not None or longitude is not None) and air_quality_stations:
        raise ValueError('Providing both coordinates and air quality stations is not supported.')

    if air_quality_stations:
        if not isinstance(air_quality_stations, list):
            air_quality_stations = [air_quality_stations]

        points = get_gfs_point_from_stations(air_quality_stations)
    elif latitude is not None:
        points = [f'ST_GEOGPOINT({longitude}, {latitude})']
    else:
        points = get_default_gfs_points()

    predicate = ' OR '.join(f'ST_EQUALS(geography, {point})' for point in points)

    query = f'''
    SELECT
      creation_time,
      ST_X(geography)                                       AS longitude,
      ST_Y(geography)                                       AS latitude,
      ARRAY_AGG(forecast_item ORDER BY forecast_item.hours) AS forecast,
      CURRENT_TIMESTAMP()                                   AS _etl_timestamp
    FROM `{GFS_PUBLIC_DATA_TABLE}`, UNNEST(forecast) AS forecast_item
    WHERE DATE(creation_time) >= @start_date
      AND DATE(creation_time) < @end_date
      AND forecast_item.hours <= @forecast_horizon_h
      AND ({predicate})
    GROUP BY 1, 2, 3
    '''

    job: bigquery.QueryJob = bigquery_merge(
        source=query,
        target=table,
        merge_keys=['creation_time', 'longitude', 'latitude'],
        wait_for_result=True,
        job_config=bigquery.QueryJobConfig(
            # clustering_fields=['geography'],
            use_legacy_sql=False,
            query_parameters=[
                bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
                bigquery.ScalarQueryParameter('end_date', 'DATE', end_date or pendulum.tomorrow()),
                bigquery.ScalarQueryParameter('forecast_horizon_h', 'INTEGER', forecast_horizon_h),
            ],
        ),
    )

    logger.info('Total bytes billed for the update query: %s MiB', job.total_bytes_billed / 1024 ** 2)


if __name__ == '__main__':
    typer.run(save_gfs_forecasts)
