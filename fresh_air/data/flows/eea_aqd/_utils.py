def _url_parse(part: int):
    def wrapped(col_name: str):
        return lambda x: x[col_name].str.split('/').str[part]

    return wrapped
