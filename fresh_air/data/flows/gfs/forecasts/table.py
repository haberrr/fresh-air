from datetime import datetime

from fresh_air.data.storage import resource_class_factory
from fresh_air.data.storage.base import SchemaField, Resource

storage_class = resource_class_factory()

table = storage_class(
    path=('gfs', 'forecasts'),
    schema=[
        SchemaField(name='creation_time', field_type=datetime),
        SchemaField(name='longitude', field_type=float),
        SchemaField(name='latitude', field_type=float),
        SchemaField(
            name='forecast',
            field_type='RECORD',
            mode='REPEATED',
            fields=[
                SchemaField(name='hours', field_type=int, mode='REQUIRED'),
                SchemaField(name='time', field_type=datetime, mode='REQUIRED'),
                SchemaField(name='temperature_2m_above_ground', field_type=float),
                SchemaField(name='specific_humidity_2m_above_ground', field_type=float),
                SchemaField(name='relative_humidity_2m_above_ground', field_type=float),
                SchemaField(name='u_component_of_wind_10m_above_ground', field_type=float),
                SchemaField(name='v_component_of_wind_10m_above_ground', field_type=float),
                SchemaField(name='total_precipitation_surface', field_type=float),
                SchemaField(name='precipitable_water_entire_atmosphere', field_type=float),
                SchemaField(name='total_cloud_cover_entire_atmosphere', field_type=float),
                SchemaField(name='downward_shortwave_radiation_flux', field_type=float)
            ],
        )
    ],
    partition_field='creation_time',
    partition_scale='month',
)
