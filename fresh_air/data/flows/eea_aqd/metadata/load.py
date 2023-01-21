from typing import Any, Dict, List

import typer
import prefect
import pandas as pd

from fresh_air.data.flows.eea_aqd.metadata.table import table, _columns_config
from fresh_air.data.flows.tasks import write_data


@prefect.task(
    name='Get EEA AQD metadata',
)
def get_eea_aqd_metadata() -> List[Dict[str, Any]]:
    URL = 'https://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv'
    return pd.read_csv(URL, sep='\t')[[
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


@prefect.flow(
    name='Load EEA AQD metadata to BigQuery',
)
def load_locations(overwrite: bool = False) -> None:
    data = get_eea_aqd_metadata()
    write_data(data, table, append=(not overwrite))


if __name__ == '__main__':
    typer.run(load_locations)
