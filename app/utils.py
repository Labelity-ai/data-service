import orjson


def json_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


json_loads = orjson.loads
