from typing import List
import s3fs
import requests

from app.services.annotations import AnnotationsService
from app.services.datasets import DatasetService
from app.models import NodeOperation, Project
from app.schema import MergeNodePayload, CVATInputNodePayload, DatasetInputNodePayload, \
    WebhookNodePayload, CVATOutputNodePayload, DatasetOutputNodePayload, QueryPipelinePost, \
    ImageAnnotationsData, MergeWebhookNodePayload, MergeType

s3_fs = s3fs.S3FileSystem()


async def run_mongo_aggregation_pipeline(pipeline: QueryPipelinePost,
                                         project: Project, *args, **kwargs) -> List[ImageAnnotationsData]:
    results = await AnnotationsService.run_annotations_pipeline(
        query=pipeline.steps, page_size=None, page=None, project=project)
    return results.data


def _webhook_call(payload: WebhookNodePayload,
                  annotations: List[ImageAnnotationsData],
                  *args, **kwargs) -> List[ImageAnnotationsData]:
    body = [x.dict(include=payload.exported_fields) for x in annotations]
    response = requests.post(url=payload.url, json=body)
    response.raise_for_status()
    response_obj = response.json()
    return [ImageAnnotationsData(**obj) for obj in response_obj]


def _webhook_call_merger(payload: MergeWebhookNodePayload,
                         left: List[ImageAnnotationsData],
                         right: List[ImageAnnotationsData],
                         *args, **kwargs) -> List[ImageAnnotationsData]:
    left_body = [x.dict(include=payload.exported_fields) for x in left]
    right_body = [x.dict(include=payload.exported_fields) for x in right]
    body = {'left': left_body, 'right': right_body}
    response = requests.post(url=payload.url, json=body)
    response.raise_for_status()
    response_obj = response.json()
    return [ImageAnnotationsData(**obj) for obj in response_obj]


def _merge_annotations_instance(left: ImageAnnotationsData, right: ImageAnnotationsData) -> ImageAnnotationsData:
    result = ImageAnnotationsData(**left.dict())
    result.tags = list(set(result.tags) | set(right.tags))
    result.points.extend(right.points)
    result.detections.extend(right.detections)
    result.polygons.extend(right.polygons)
    result.polylines.extend(right.polylines)
    result.captions.extend(right.captions)
    result.attributes = {**right.attributes, **left.attributes}
    # TODO: combine labels
    return result


def _merge_annotations(payload: MergeNodePayload,
                       left: List[ImageAnnotationsData],
                       right: List[ImageAnnotationsData],
                       *args, **kwargs) -> List[ImageAnnotationsData]:
    left_dict = {annotations.event_id: annotations for annotations in left}
    right_dict = {annotations.event_id: annotations for annotations in right}
    all_event_ids = set(right_dict.keys()) | set(left_dict.keys())
    results = []

    if payload.merge_type == MergeType.LEFT_APPEND:
        for annotations in left:
            if annotations.event_id in right_dict:
                results.append(_merge_annotations_instance(annotations, right_dict[annotations.event_id]))
            else:
                results.append(annotations)

    if payload.merge_type == MergeType.RIGHT_APPEND:
        for annotations in right:
            if annotations.event_id in left_dict:
                results.append(_merge_annotations_instance(annotations, left_dict[annotations.event_id]))
            else:
                results.append(annotations)

    if payload.merge_type == MergeType.OUTER_APPEND:
        for event_id in all_event_ids:
            if event_id in left_dict and event_id in right_dict:
                results.append(_merge_annotations_instance(left_dict[event_id], right_dict[event_id]))
            elif event_id in left_dict:
                results.append(left_dict[event_id])
            else:
                results.append(right_dict[event_id])

    if payload.merge_type == MergeType.CUSTOM_WEBHOOK:
        results = _webhook_call_merger(MergeWebhookNodePayload(**payload.parameters), left, right)

    return results


def _fetch_dataset_snapshot(payload: DatasetInputNodePayload,
                            project: Project, *args, **kwargs) -> List[ImageAnnotationsData]:
    return await DatasetService.get_annotations_by_dataset_id(
        dataset_id=payload.dataset_id, project_id=project.id)


OUTPUT_NODE_OPERATIONS = {
    NodeOperation.CVAT: (None, CVATOutputNodePayload),
    NodeOperation.DATASET: (None, DatasetOutputNodePayload),
    NodeOperation.WEBHOOK: (_webhook_call, WebhookNodePayload),
    NodeOperation.ANNOTATIONS: (None, dict),
    # NodeOperation.QUERY_PIPELINE: (None, None),
    NodeOperation.MERGE: (_merge_annotations, MergeNodePayload),
    NodeOperation.REVISION: (None, None)
}

INPUT_NODE_OPERATIONS = {
    NodeOperation.CVAT: (None, CVATInputNodePayload),
    NodeOperation.DATASET: (_fetch_dataset_snapshot, DatasetInputNodePayload),
    # NodeOperation.WEBHOOK: (None, WebhookNodePayload),
    # NodeOperation.ANNOTATIONS: (None, dict),
    NodeOperation.QUERY_PIPELINE: (run_mongo_aggregation_pipeline, QueryPipelinePost),
    # NodeOperation.MERGE: (None, MergeNodePayload),
    NodeOperation.REVISION: (None, None)
}
