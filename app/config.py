import os


class Config:
    POST_BULK_LIMIT = os.environ.get('POST_BULK_LIMIT', 1000)
    MONGO_HOST = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
    MONGO_DATABASE = os.environ.get('MONGO_URI', 'default_db')
