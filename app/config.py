import os
from dotenv import load_dotenv


load_dotenv()


class Config:
    POST_BULK_LIMIT = os.environ.get('POST_BULK_LIMIT', 1000)
    MONGO_HOST = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
    MONGO_DATABASE = os.environ.get('MONGO_DATABASE', 'default_database')
    IMAGE_STORAGE_BUCKET = os.environ.get('IMAGE_STORAGE_BUCKET', 'labelity-use1-dev-user-storage')
    RAW_IMAGES_FOLDER = os.environ.get('RAW_IMAGES_FOLDER', 'raw')
    THUMBNAILS_FOLDER = os.environ.get('THUMBNAILS_FOLDER', 'thumbnails')
    OPTIMIZED_IMAGES_FOLDER = os.environ.get('OPTIMIZED_IMAGES_FOLDER', 'optimized')
    SIGNED_POST_URL_EXPIRATION = os.environ.get('SIGNED_POST_URL_EXPIRATION', 100)
    SECRET_KEY = os.environ.get('SECRET_KEY', 'b308c184a53819197ae49b274ae10dcf0a590ee2659925fd440c918ce30f96ea')
