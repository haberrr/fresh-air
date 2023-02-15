from fresh_air.data.flows._utils import _url_parse, _to_timestamp
from fresh_air.data.storage import resource_class_factory
from fresh_air.data.storage.base import SchemaField, Resource

storage_class = resource_class_factory()

_columns_config = [
    dict(
        name='AirQualityStation',
        field=SchemaField(
            name='air_quality_station',
            field_type=str,
        ),
        primary_key=True,
    ),
    dict(
        name='AirPollutantCode',
        field=SchemaField(
            name='air_pollutant_code',
            field_type=int,
        ),
        preprocess=_url_parse(-1),
        convert=True,
        primary_key=True,
    ),
    dict(
        name='DatetimeBegin',
        field=SchemaField(
            name='measurement_ts',
            field_type='timestamp',
        ),
        preprocess=_to_timestamp,
        primary_key=True,
    ),
    dict(
        name='Concentration',
        field=SchemaField(
            name='concentration',
            field_type=float,
        ),
    ),
    dict(
        name='Validity',
        field=SchemaField(
            name='validity',
            field_type=int,
        ),
    ),
    dict(
        name='Verification',
        field=SchemaField(
            name='verification',
            field_type=int,
        ),
    ),
]

meta_table = storage_class(
    path=('eea_aqd', 'measurement_report_urls'),
    schema=[
        SchemaField(name='report_url', field_type=str),
    ],
)

table = storage_class(
    path=('eea_aqd', 'measurements'),
    schema=[col['field'] for col in _columns_config],
    partition_field='measurement_ts',
    partition_scale='MONTH',
)


def stg_table_factory(flow_id: str) -> Resource:
    """
    Factory for producing temporary table for storing raw measurements data downloaded from EEA AQD.

    Args:
        flow_id: ID of the Prefect flow, serves as a suffix for a table name.

    Returns:
        Resource object for the table.
    """
    return storage_class(
        path=('stg', f'eea_aqd__measurements__{flow_id}'),
        schema=[col['field'] for col in _columns_config],
    )
