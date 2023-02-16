from datetime import datetime
from typing import Optional, List

import prefect
import typer
from google.cloud import bigquery

from fresh_air.data.flows.gfs.default_points.table import table
from fresh_air.data.flows.eea_aqd.metadata.table import table as eea_aqd_metadata
from fresh_air.data.flows.eea_aqd.measurements.table import table as eea_aqd_measurements
from fresh_air.data.storage import BigQueryTable


@prefect.flow(
    name='Save default GFS points',
)
def save_default_gfs_points(
        measurements_start_date: datetime = '2023-01-01',
        measurements_end_date: datetime = '2023-01-31',
        country_codes: Optional[List[str]] = None,
        air_pollutant_codes: Optional[List[int]] = None,
) -> None:
    """
    Prepare default GFS grid points from EEA AQD measurements data.

    Args:
        measurements_start_date: Include stations with measurement data starting from this date.
        measurements_end_date: Include stations with measurement data up to this date.
        country_codes: Include stations from this countries.
        air_pollutant_codes: Include stations with measurements of this pollutant codes.
    """
    if not isinstance(eea_aqd_metadata, BigQueryTable) \
            or not isinstance(eea_aqd_measurements, BigQueryTable) \
            or not isinstance(table, BigQueryTable):
        raise NotImplementedError('Only BigQueryTable storage is supported.')

    measurements_start_date = measurements_start_date.date()
    measurements_end_date = measurements_end_date.date()

    country_predicate = 'AND country_code IN UNNEST(@country_codes)' if country_codes else ''
    air_pollutant_predicate = 'AND air_pollutant_code IN UNNEST(@air_pollutant_codes)' if air_pollutant_codes else ''

    query = f'''
    CREATE OR REPLACE TABLE `{table.full_table_name}`
    AS
    SELECT
        longitude_round     AS longitude,
        latitude_round      AS latitude,
        CURRENT_TIMESTAMP() AS _etl_timestamp
    FROM (SELECT DISTINCT
              latitude_round
            , longitude_round
          FROM (SELECT
                    air_quality_station
                  , ROUND(AVG(latitude) * 4) / 4  AS latitude_round
                  , ROUND(AVG(longitude) * 4) / 4 AS longitude_round
                FROM `{eea_aqd_metadata.full_table_name}`
                WHERE TRUE
                  {country_predicate}
                GROUP BY air_quality_station
                HAVING COUNT(DISTINCT country_code) = 1
                  AND COUNT(DISTINCT ROUND(latitude * 4) / 4) = 1
                  AND COUNT(DISTINCT ROUND(longitude * 4) / 4) = 1)
          JOIN (SELECT DISTINCT
                    air_quality_station
                FROM `{eea_aqd_measurements.full_table_name}`
                WHERE DATE(measurement_ts) >= @measurement_start_date
                  AND DATE(measurement_ts) <= @measurement_end_date)
                  {air_pollutant_predicate}
              USING (air_quality_station));
    '''

    # noinspection PyTypeChecker
    table.run_query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('measurement_start_date', 'DATE', measurements_start_date),
                bigquery.ScalarQueryParameter('measurement_end_date', 'DATE', measurements_end_date),
                bigquery.ArrayQueryParameter('country_codes', 'STRING', country_codes),
                bigquery.ArrayQueryParameter('air_pollutant_codes', 'STRING', air_pollutant_codes),
            ]
        ),
        wait_for_result=True,
    )


if __name__ == '__main__':
    typer.run(save_default_gfs_points)
