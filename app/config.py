import os


class Config:
    POST_BULK_LIMIT = os.environ.get('POST_BULK_LIMIT', 1000)
