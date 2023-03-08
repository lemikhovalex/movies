from orjson import dumps


def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, pydantic requires unicode,
    # so here we convert
    return dumps(v, default=default).decode()
