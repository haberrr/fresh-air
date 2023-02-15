import os

import pandas as pd


def _url_parse(part: int):
    """Retrieve `part`-th part of the URL when split by '/'."""

    def wrapped(col_name: str):
        return lambda x: x[col_name].str.split('/').str[part]

    return wrapped


def _to_timestamp(col_name: str):
    """Convert to timestamp."""

    def processor(x: pd.DataFrame):
        return pd.to_datetime(x[col_name]).dropna().view(int) / 1e9

    return processor


def _read_query_from_neighbour(path: str, filename: str) -> str:
    """Read query from the neighbour to the `file` (usually __file__) with name `filename`."""

    with open(os.path.join(os.path.dirname(path), filename)) as f:
        return f.read()
