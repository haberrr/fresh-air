def _url_parse(part: int):
    """Retrieve `part`-th part of the URL when split by '/'."""
    def wrapped(col_name: str):
        return lambda x: x[col_name].str.split('/').str[part]

    return wrapped
