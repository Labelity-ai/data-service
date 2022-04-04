import orjson
import os


def json_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


def zip_dir(path, zip_handler):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip_handler.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))


json_loads = orjson.loads
