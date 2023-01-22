from fresh_air.data.flows.eea_aqd._utils import _url_parse
from fresh_air.data.storage import resource_class_factory
from fresh_air.data.storage.base import SchemaField

storage_class = resource_class_factory()

_columns_config = [
    dict(
        name='Countrycode',
        field=SchemaField(
            name='country_code',
            field_type=str,
        ),
    ),
    dict(
        name='AirQualityStation',
        field=SchemaField(
            name='air_quality_station',
            field_type=str,
        ),
    ),
    dict(
        name='AirQualityStationEoICode',
        field=SchemaField(
            name='air_quality_station_code',
            field_type=str,
        ),
    ),
    dict(
        name='AirPollutant',
        field=SchemaField(
            name='air_pollutant',
            field_type=str,
        ),
    ),
    dict(
        name='AirPollutantCode',
        field=SchemaField(
            name='air_pollutant_code',
            field_type=int,
        ),
        preprocess=_url_parse(-1),
        convert=True,
    ),
    dict(
        name='AveragingTime',
        field=SchemaField(
            name='averaging_time',
            field_type=str,
        ),
    ),
    dict(
        name='Concentration',
        field=SchemaField(
            name='concentration',
            field_type=float,
        ),
    ),
    dict(
        name='UnitOfMeasurement',
        field=SchemaField(
            name='unit_of_measurement',
            field_type=str,
        ),
    ),
    dict(
        name='DatetimeBegin',
        field=SchemaField(
            name='begin_ts',
            field_type='timestamp',
        ),
    ),
    dict(
        name='DatetimeEnd',
        field=SchemaField(
            name='end_ts',
            field_type='timestamp',
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

table = storage_class(
    path=('eea_aqd', 'measurements'),
    schema=[col['field'] for col in _columns_config],
    partition_field='begin_ts',
    partition_scale='MONTH',
)
