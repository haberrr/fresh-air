from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Literal

import prefect

from turbiner.data.flows.openaq.measurements.table import measurements_table
from turbiner.data.flows.tasks import query_bigquery, write_to_bigquery
from turbiner.data.connections.openaq import OpenAQ

_VIABLE_LOCATIONS_QUERY = '''
SELECT id
FROM openaq.locations
WHERE lastUpdated >= (CURRENT_TIMESTAMP() - INTERVAL 30 DAY)
  AND firstUpdated <= '2021-12-01'
  AND DATE_DIFF(lastUpdated, firstUpdated, MINUTE) / (measurements / parameters_count) < 90
  AND NOT isMobile
  AND pm25_flg
  AND pm10_flg
  AND sensorType = 'reference grade'
'''


@prefect.task(
    name='List viable OpenAQ locations from BigQuery',
    description='Retrieve a filtered list of locations from BigQuery.',
    tags=['bigquery'],
    cache_key_fn=prefect.tasks.task_input_hash,
    cache_expiration=timedelta(hours=3),
)
def get_locations() -> List[int]:
    return [row['id'] for row in query_bigquery.fn(_VIABLE_LOCATIONS_QUERY)]


@prefect.task(
    name='Get OpenAQ measurements',
    description='Get OpenAQ measurement data from `averages` endpoint.',
    tags=['openaq'],
    retries=5,
    retry_delay_seconds=15,
    cache_key_fn=prefect.tasks.task_input_hash,
    cache_expiration=timedelta(minutes=45),
)
def get_averages(
        date_start: datetime,
        date_end: datetime,
        location: int,
        page_size: int,
        temporal_scale: Literal['hour', 'day', 'month', 'year'] = 'hour',
        openaq_client: Optional[OpenAQ] = None,
) -> List[Dict[str, Any]]:
    openaq_client = openaq_client or OpenAQ(page_size=page_size)
    return openaq_client.averages(
        date_from=date_start,
        date_to=date_end,
        location=location,
        temporal=temporal_scale,
        lazy=False,
    )


@prefect.task(
    name='Preprocess OpenAQ measurements data',
)
def preprocess_averages(data: List[Dict[str, Any]], temporal_scale: str) -> List[Dict[str, Any]]:
    result = []
    for row in data:
        result.append({
            'id': row.get('name'),
            'measurement_ts': row.get(temporal_scale),
            'value': row.get('average'),
            'unit': row.get('unit'),
            'parameter': row.get('parameter'),
            'measurement_count': row.get('measurement_count'),
            'temporal_scale': temporal_scale,
        })

    return result


@prefect.flow(
    name='Load OpenAQ location measurements part',
    description='Extract and load small portion of average measurements for one location.',
    retries=5,
    retry_delay_seconds=30,
)
def load_location_measurements_part(
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
        location: int = None,
        temporal_scale: Literal['hour', 'day', 'month', 'year'] = 'hour',
        page_size: int = 2000,
):
    raw_future = get_averages.submit(
        date_start=date_start,
        date_end=date_end,
        location=location,
        page_size=page_size,
    )
    preprocessed_future = preprocess_averages.submit(raw_future, temporal_scale)
    write_to_bigquery.submit(
        preprocessed_future,
        measurements_table,
    )


@prefect.flow(
    name='Load OpenAQ location measurements',
    description='Extract and load average measurements for one location.',
)
def load_location_measurements(
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
        location: int = None,
        page_size: int = 2000,
        time_step_hours: int = 24 * 7,
        temporal_scale: Literal['hour', 'day', 'month', 'year'] = 'hour',
):
    date_start = date_start or datetime.now() - timedelta(hours=3)
    date_end = date_end or datetime.now()

    time_step = timedelta(hours=time_step_hours)

    while date_start <= date_end:
        load_location_measurements_part(
            date_start=date_start,
            date_end=min(date_start + time_step, date_end),
            location=location,
            page_size=page_size,
            temporal_scale=temporal_scale,
        )

        date_start += time_step


@prefect.flow(
    name='Load OpenAQ measurements',
    description='Load measurements from OpenAQ `averages` endpoint as specified by filters.',
    retries=5,
    retry_delay_seconds=30,
)
def load_measurements(
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
        locations: Optional[List[int]] = None,
        page_size: int = 2000,
        time_step_hours: int = 24 * 7,
        temporal_scale: Literal['hour', 'day', 'month', 'year'] = 'hour',
):
    date_start = date_start or datetime.now() - timedelta(hours=3)
    date_end = date_end or datetime.now()
    locations = locations or get_locations()

    for location in locations:
        load_location_measurements(
            date_start=date_start,
            date_end=date_end,
            location=location,
            page_size=page_size,
            time_step_hours=time_step_hours,
            temporal_scale=temporal_scale,
            return_state=True,
        )
