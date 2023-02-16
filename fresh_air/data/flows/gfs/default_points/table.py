from fresh_air.data.storage import resource_class_factory
from fresh_air.data.storage.base import SchemaField

storage_class = resource_class_factory()

table = storage_class(
    path=('gfs', 'default_points'),
    schema=[
        SchemaField(name='longitude', field_type=float),
        SchemaField(name='latitude', field_type=float),
    ],
)
