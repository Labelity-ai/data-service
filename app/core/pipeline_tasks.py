from typing import List
import s3fs
from rq.decorators import job
import requests

from app.services.annotations import AnnotationsService
from app.models import ImageAnnotations
from app.config import Config
from app.core.queue import redis

s3_fs = s3fs.S3FileSystem()


@job(queue='pipelines', connection=redis)
def human_in_the_loop(annotations: List[ImageAnnotations]):
    s3_fs.open(f'{Config.DATASET_ARTIFACTS_BUCKET}/{Config.DATASET_CACHE_FOLDER}/')
    # Store annotations in S3
    # Generate link
    # Send notification
    pass


@job(queue='pipelines', connection=redis)
def run_mongo_aggregation_pipeline(pipeline, project_id):
    AnnotationsService.run_raw_annotations_pipeline(
        pipeline, page_size=None, page=None, project_id=project_id)


def webhook_call(annotations: List[ImageAnnotations], webhook: str) -> List[ImageAnnotations]:
    body = [x.dict() for x in annotations]
    response = requests.post(url=webhook, json=body)
    response.raise_for_status()
    response_obj = response.json()
    return [ImageAnnotations(**obj) for obj in response_obj]


@job(queue='pipelines', connection=redis)
def data_augmentation():
    pass
