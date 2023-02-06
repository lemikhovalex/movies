from orjson import dumps


def orjson_dumps(v, *, default):
    # orjson.dumps возвращает bytes, а pydantic требует unicode,
    # поэтому декодируем
    return dumps(v, default=default).decode()
