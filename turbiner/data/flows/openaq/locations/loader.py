import prefect

from turbiner.data.connections.openaq import OpenAQ
from turbiner.data.flows.tasks import write_to_bigquery
from turbiner.data.storage.bigquery import BigQueryTable
from turbiner.data.storage.base import SchemaField

locations_table = BigQueryTable(
    path=(None, 'openaq', 'locations'),
    schema=[
        SchemaField(
            name='id',
            field_type='int',
            description='Unique ID for the location.',
        ),
        SchemaField(
            name='country',
            field_type='string',
            description='Country-code of the measurement location.',
        ),
        SchemaField(
            name='city',
            field_type='string',
            description='Unique ID for the location.',
        ),
        SchemaField(
            name='name',
            field_type='string',
            description='Measurement location name.',
        ),
        SchemaField(
            name='entity',
            field_type='string',
            description='Source entity type (government, community, research).',
        ),
        SchemaField(
            name='sensorType',
            field_type='string',
            description='Type of measurement sensor (low-cost, reference grade).',
        ),
        SchemaField(
            name='latitude',
            field_type='float',
            description='Latitude of the measurement location.',
        ),
        SchemaField(
            name='longitude',
            field_type='float',
            description='Longitude of the measurement location.',
        ),
        SchemaField(
            name='isMobile',
            field_type='bool',
            description='Whether this is mobile location.',
        ),
        SchemaField(
            name='isAnalysis',
            field_type='bool',
            description='Whether this is actual measurements or result of analysis.',
        ),
        SchemaField(
            name='pm25_flg',
            field_type='bool',
            description='Whether this location measures PM2.5 concentration.',
        ),
        SchemaField(
            name='pm10_flg',
            field_type='bool',
            description='Whether this location measures PM10 concentration.',
        ),
        SchemaField(
            name='no2_flg',
            field_type='bool',
            description='Whether this location measures NO2 concentration.',
        ),
        SchemaField(
            name='so2_flg',
            field_type='bool',
            description='Whether this location measures SO2 concentration.',
        ),
        SchemaField(
            name='o3_flg',
            field_type='bool',
            description='Whether this location measures O3 concentration.',
        ),
        SchemaField(
            name='co_flg',
            field_type='bool',
            description='Whether this location measures CO concentration.',
        ),
        SchemaField(
            name='parameters_count',
            field_type='int',
            description='Number of measured parameters at this location.',
        ),
        SchemaField(
            name='measurements',
            field_type='int',
            description='Number of total measurements recorded for this location.',
        ),
        SchemaField(
            name='firstUpdated',
            field_type='timestamp',
            description='Timestamp of the first recorded measurement.',
        ),
        SchemaField(
            name='lastUpdated',
            field_type='timestamp',
            description='Timestamp of the last recorded measurement.',
        ),
    ],
    clustering_fields=['country', 'city', 'lastUpdated'],
)


@prefect.task(
    name='Get OpenAQ locations',
    description='Download locations data from OpenAQ API.',
    tags=['openaq'],
)
def get_openaq_locations():
    openaq = OpenAQ(page_size=2000, pbar=True)

    data = []
    for loc in openaq.locations():
        parameters = [item.get('parameter') for item in (loc.get('parameters') or [])]

        data.append({
            'id': loc.get('id'),
            'country': loc.get('country'),
            'city': loc.get('city'),
            'name': loc.get('name'),
            'entity': loc.get('entity'),
            'sensorType': loc.get('sensorType'),
            'latitude': (loc.get('coordinates') or {}).get('latitude'),
            'longitude': (loc.get('coordinates') or {}).get('longitude'),
            'isMobile': loc.get('isMobile'),
            'isAnalysis': loc.get('isAnalysis'),
            'pm25_flg': 'pm25' in parameters,
            'pm10_flg': 'pm10' in parameters,
            'no2_flg': 'no2' in parameters,
            'so2_flg': 'so2' in parameters,
            'o3_flg': 'o3' in parameters,
            'co_flg': 'co' in parameters,
            'parameters_count': len(parameters),
            'measurements': loc.get('measurements'),
            'firstUpdated': loc.get('firstUpdated'),
            'lastUpdated': loc.get('lastUpdated'),
        })

    return data


@prefect.flow(
    name='Load OpenAQ locations from API to BigQuery',
)
def load_locations():
    data = get_openaq_locations()
    write_to_bigquery(data, locations_table, append=False)


if __name__ == '__main__':
    load_locations()
