import os
from dotenv import load_dotenv


load_dotenv()


class Config:
    POST_BULK_LIMIT = int(os.environ.get('POST_BULK_LIMIT', 1000))
    MONGO_HOST = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
    MONGO_DATABASE = os.environ.get('MONGO_DATABASE', 'default_database')
    IMAGE_STORAGE_BUCKET = os.environ.get('IMAGE_STORAGE_BUCKET', 'labelity-use-dev-images')
    RAW_IMAGES_FOLDER = os.environ.get('RAW_IMAGES_FOLDER', 'raw')
    VIDEOS_FOLDER = os.environ.get('VIDEOS_FOLDER', 'videos')
    THUMBNAILS_FOLDER = os.environ.get('THUMBNAILS_FOLDER', 'thumbnails')
    DATASET_ARTIFACTS_BUCKET = os.environ.get('DATASET_ARTIFACTS_BUCKET', 'labelity-use-user-datasets')
    DATASET_EXPORTING_QUEUE_FOLDER = os.environ.get('DATASET_EXPORTING_QUEUE_FOLDER', 'queue')
    DATASET_EXPORTING_RESULTS_FOLDER = os.environ.get('DATASET_EXPORTING_RESULTS_FOLDER', 'results')
    SIGNED_POST_URL_EXPIRATION = int(os.environ.get('SIGNED_POST_URL_EXPIRATION', 100))
    SIGNED_GET_THUMBNAIL_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_THUMBNAIL_URL_EXPIRATION ', 100))
    SIGNED_GET_IMAGE_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_IMAGE_URL_EXPIRATION ', 100))
    SIGNED_GET_OBJECT_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_OBJECT_URL_EXPIRATION ', 100))
    SECRET_KEY = os.environ.get('SECRET_KEY', 'b308c184a53819197ae49b274ae10dcf0a590ee2659925fd440c918ce30f96ea')
    DATASET_TOKEN_DEFAULT_TIMEDELTA_HOURS = int(os.environ.get('DATASET_TOKEN_DEFAULT_TIMEDELTA_MINUTES', 24 * 30))
    VIDEO_FPS_LIMIT = int(os.environ.get('VIDEO_FPS_LIMIT', 5))
    JWT_ALGORITHM = "HS512"
