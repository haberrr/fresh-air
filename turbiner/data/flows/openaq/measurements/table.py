from turbiner.data.storage.base import SchemaField
from turbiner.data.storage.bigquery import BigQueryTable

measurements_table = BigQueryTable(
    path=(None, 'openaq', 'measurements'),
    schema=[
        SchemaField(
            name='id',
            field_type='int',
            description='Measurement location ID.',
        ),
        SchemaField(
            name='measurement_ts',
            field_type='timestamp',
            description='Measurement time-bucket timestamp.',
        ),
        SchemaField(
            name='value',
            field_type='float',
            description='Average measurement value',
        ),
        SchemaField(
            name='unit',
            field_type='string',
            description='Measurement unit.'
        ),
        SchemaField(
            name='parameter',
            field_type='string',
            description='Measurement parameter.',
        ),
        SchemaField(
            name='measurement_count',
            field_type='int',
            description='Number of measurements within current time-bucket.',
        ),
        SchemaField(
            name='temporal_scale',
            field_type='string',
            description='Temporal scale for averaging.'
        ),
    ],
)
