from typing import List
import s3fs
from prefect import task
from app.services.annotations import AnnotationsService
from app.models import ImageAnnotations
from app.config import Config

s3_fs = s3fs.S3FileSystem()


@task
def human_in_the_loop(annotations: List[ImageAnnotations]):
    s3_fs.open(f'{Config.DATASET_ARTIFACTS_BUCKET}/{Config.DATASET_CACHE_FOLDER}/{}')
    # Store annotations in S3
    # Generate link
    # Send notification
    pass


@task
def run_mongo_aggregation_pipeline(pipeline, project_id):
    AnnotationsService.run_raw_annotations_pipeline(
        pipeline, page_size=None, page=None, project_id=project_id)


@task
def data_augmentation():
    pass


@task
def run_inference():
    pass
