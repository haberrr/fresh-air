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
    """
    Request EEA AQD metadata.

    Metadata contains information on measurement stations and the pollutant types they are measuring, their dates
     of operation and equipment.

    Returns:
        List of records, where each record contains information on one pollutant measurement attributes by one station.
    """
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
    name='Load EEA AQD metadata',
)
def load_metadata(overwrite: bool = False) -> None:
    """Load and save EEA AQD metadata to storage."""
    data = get_eea_aqd_metadata()
    write_data(data, table, append=(not overwrite))


if __name__ == '__main__':
    typer.run(load_metadata)
