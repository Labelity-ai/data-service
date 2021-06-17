from prefect import task
from app.services.annotations import AnnotationsService


@task
def human_in_the_loop_in(annotations):
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
