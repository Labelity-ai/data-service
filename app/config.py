import os
from enum import Enum
from dotenv import load_dotenv

load_dotenv()


class TracingSampler(Enum):
    ALWAYS = 'always'
    PROBABILISTIC = 'probabilistic'


class Config:
    # Mongo Config
    MONGO_HOST = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
    MONGO_DATABASE = os.environ.get('MONGO_DATABASE', 'default_database')

    # AWS Config
    AWS_ENDPOINT_URL = os.environ.get('AWS_ENDPOINT_URL')

    # Security Config
    SECRET_KEY = os.environ.get('SECRET_KEY', 'b308c184a53819197ae49b274ae10dcf0a590ee2659925fd440c918ce30f96ea')
    JWT_ALGORITHM = "HS512"
    FAST_TOKEN_JWT_ALGORITHM = "HS256"
    DATASET_TOKEN_DEFAULT_TIMEDELTA_HOURS = int(os.environ.get('DATASET_TOKEN_DEFAULT_TIMEDELTA_MINUTES', 24 * 30))

    # Redis Config
    REDIS_HOST = os.environ['REDIS_HOST']
    REDIS_PORT = os.environ['REDIS_PORT']
    REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
    REDIS_DATABASE = os.environ['REDIS_DATABASE']

    # Tracing Config
    TRACING_SAMPLER: TracingSampler = TracingSampler.ALWAYS

    # Buckets Config
    IMAGE_STORAGE_BUCKET = os.environ['IMAGE_STORAGE_BUCKET']
    DATASET_ARTIFACTS_BUCKET = os.environ['DATASET_ARTIFACTS_BUCKET']
    PIPELINES_BUCKET = os.environ['PIPELINES_BUCKET']

    # Documents Storage Config
    RAW_IMAGES_FOLDER = os.environ.get('RAW_IMAGES_FOLDER', 'raw')
    VIDEOS_FOLDER = os.environ.get('VIDEOS_FOLDER', 'videos')
    THUMBNAILS_FOLDER = os.environ.get('THUMBNAILS_FOLDER', 'thumbnails')

    # Dataset Storage Config
    DATASET_EXPORTING_QUEUE_FOLDER = os.environ.get('DATASET_EXPORTING_QUEUE_FOLDER', 'datasets/queue')
    DATASET_EXPORTING_RESULTS_FOLDER = os.environ.get('DATASET_EXPORTING_RESULTS_FOLDER', 'datasets/compiled')
    DATASET_CACHE_FOLDER = os.environ.get('DATASET_CACHE_FOLDER', 'datasets/cache')
    DATASET_SNAPSHOT_FOLDER = os.environ.get('DATASET_EXPORTING_RESULTS_FOLDER', 'datasets/snapshots')

    # Pipelines Storage Config
    PIPELINES_LOGS_FOLDER = os.environ.get('PIPELINES_LOGS_FOLDER' 'logs')
    PIPELINES_RESULTS_FOLDER = os.environ.get('PIPELINES_RESULTS_FOLDER' 'results')

    # Rate Limiting
    POST_BULK_LIMIT = int(os.environ.get('POST_BULK_LIMIT', 1000))
    VIDEO_FPS_LIMIT = int(os.environ.get('VIDEO_FPS_LIMIT', 5))

    THUMBNAILS_MAX_WIDTH = int(os.environ.get('THUMBNAILS_WIDTH', '500'))
    THUMBNAILS_MAX_HEIGHT = int(os.environ.get('THUMBNAILS_MAX_HEIGHT', '500'))

    SIGNED_POST_URL_EXPIRATION = int(os.environ.get('SIGNED_POST_URL_EXPIRATION', 3600))
    SIGNED_GET_THUMBNAIL_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_THUMBNAIL_URL_EXPIRATION ', 3600))
    SIGNED_GET_IMAGE_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_IMAGE_URL_EXPIRATION ', 3600))
    SIGNED_GET_OBJECT_URL_EXPIRATION = int(os.environ.get('SIGNED_GET_OBJECT_URL_EXPIRATION ', 3600))
