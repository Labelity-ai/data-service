from typing import List, Optional, Union
from enum import Enum

from fastapi import HTTPException
from odmantic import ObjectId
from pymongo import ASCENDING, DESCENDING

from app.schema import ImageAnnotationsPostSchema, AnnotationsQueryResult,\
    PredictionPostData, CaptionPostData, ImageAnnotationsPatchSchema, ImageAnnotationsPutSchema
from app.models import ImageAnnotations, Project, engine, Image, Prediction, Caption
from app.core.importers import DatasetImportFormat, import_dataset
from app.core.query_engine.stages import STAGES, QueryStage, make_paginated_pipeline
from app.services.projects import ProjectService
from app.services.storage import StorageService
from app.core.tracing import traced


def _add_group_to_annotations(annotations: List[Union[PredictionPostData, CaptionPostData]], group: str):
    return [{**x.dict(), 'group': group} for x in annotations]


def _replace_annotations(previous: List[Union[Prediction, Caption]],
                         new: List[Union[PredictionPostData, CaptionPostData]], group: str):
    new_annotations = _add_group_to_annotations(new, group)
    return [x for x in previous if x.group != group] + new_annotations


class AnnotationSortField(str, Enum):
    IMAGE_NAME = 'event_id'
    # CREATED_TIME = 'created_time'
    # UPDATED_TIME = 'updated_time'


class AnnotationSortDirection(str, Enum):
    ASCENDING = 'ascending'
    DESCENDING = 'descending'


@traced
class AnnotationsService:
    @staticmethod
    async def get_annotations_by_event_id(event_id: str, project_id: ObjectId) -> ImageAnnotations:
        annotations = await engine.find_one(
            ImageAnnotations,
            (ImageAnnotations.project_id == project_id) & (ImageAnnotations.event_id == event_id))

        if annotations is None:
            raise HTTPException(404)

        return annotations

    @staticmethod
    async def run_raw_annotations_pipeline(pipeline: List[dict],
                                           page_size: Optional[int],
                                           page: Optional[int],
                                           project_id: ObjectId) -> AnnotationsQueryResult:
        pipeline = [{'$match': {'project_id': project_id}}] + pipeline
        pipeline = make_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)

        for item in result['data']:
            image = item['image'] and item['image'][0]

            if image:
                item['thumbnail_url'] = await StorageService.create_presigned_get_url_for_thumbnail(
                    item['event_id'], project_id)
                item['image_url'] = await StorageService.create_presigned_get_url_for_image(
                    item['event_id'], project_id)
                item['image_width'] = image['width']
                item['image_height'] = image['height']
                item['has_image'] = True

        pagination = result['metadata'][0] if result['metadata'] else {'page': 0, 'total': 0}
        data = result['data']

        return AnnotationsQueryResult(data=data, pagination=pagination)

    @staticmethod
    async def get_annotations(page_size: int,
                              page: int,
                              only_with_images: bool,
                              sort_field: AnnotationSortField,
                              sort_direction: AnnotationSortDirection,
                              project: Project) -> AnnotationsQueryResult:
        pipeline = []

        if only_with_images:
            pipeline += [{'$match': {'has_image': True}}]

        sort_dir = DESCENDING if sort_direction == AnnotationSortDirection.DESCENDING else ASCENDING
        pipeline += [{'$sort': {sort_field.value: sort_dir}}]

        return await AnnotationsService.run_raw_annotations_pipeline(
            pipeline, page_size=page_size, page=page, project_id=project.id)

    @staticmethod
    async def run_annotations_pipeline(query: List[QueryStage],
                                       page_size: Optional[int],
                                       page: Optional[int],
                                       project: Project) -> AnnotationsQueryResult:
        pipeline = []

        project_labels = await ProjectService.get_project_labels(project.id)
        project_attributes = await ProjectService.get_project_attributes(project.id)

        for step in query:
            try:
                stage = STAGES[step.stage.value](**step.parameters)
                stage.validate_stage(project_labels=project_labels, project_attributes=project_attributes)
                pipeline.extend(stage.to_mongo())
            except ValueError as error:
                print(error)
                raise HTTPException(400, detail=str(error))

        return await AnnotationsService.run_raw_annotations_pipeline(
            pipeline, page_size=page_size, page=page, project_id=project.id)

    @staticmethod
    async def add_annotations(annotation: ImageAnnotationsPostSchema,
                              replace: bool, group: str, project_id: ObjectId) -> ImageAnnotations:
        result = await AnnotationsService.add_annotations_bulk([annotation], replace, group, project_id)
        return result[0]

    @staticmethod
    async def update_annotations(instance: ImageAnnotations,
                                 annotation: Union[ImageAnnotationsPatchSchema, ImageAnnotationsPutSchema],
                                 group: str) -> ImageAnnotations:
        for attribute in ['tags', 'points', 'polygons', 'polylines', 'captions', 'detections']:
            new_data = getattr(annotation, attribute)

            if new_data is not None:
                if not group:
                    new = getattr(annotation, attribute)
                else:
                    new = _replace_annotations(getattr(instance, attribute),
                                               getattr(annotation, attribute),
                                               group=group)
                setattr(instance, attribute, new)

        instance.labels = instance.get_labels()
        return await engine.save(instance)

    @staticmethod
    async def add_annotations_bulk(annotations: List[ImageAnnotationsPostSchema],
                                   replace: bool, group: str, project_id: ObjectId) -> List[ImageAnnotations]:
        event_ids = [annotation.event_id for annotation in annotations]

        previous_instances = await engine.find(
            ImageAnnotations,
            ImageAnnotations.project_id == project_id,
            ImageAnnotations.event_id.in_(event_ids))

        event_id_to_instance = {ins.event_id: ins for ins in previous_instances}

        images = await engine.find(Image, Image.event_id.in_(event_ids))
        images = {ins.event_id: True for ins in images}

        result = []

        for annotation in annotations:
            if annotation.event_id in event_id_to_instance:
                instance = event_id_to_instance.get(annotation.event_id)
            else:
                instance = ImageAnnotations(event_id=annotation.event_id, project_id=project_id)

            if replace:
                instance.detections = _replace_annotations(instance.detections, annotation.detections, group)
                instance.polygons = _replace_annotations(instance.polygons, annotation.polygons, group)
                instance.points = _replace_annotations(instance.points, annotation.points, group)
                instance.tags = _replace_annotations(instance.tags, annotation.tags, group)
                instance.polylines = _replace_annotations(instance.polylines, annotation.polylines, group)
                instance.captions = _replace_annotations(instance.captions, annotation.captions, group)
            else:
                instance.detections += _add_group_to_annotations(annotation.detections, group)
                instance.polygons += _add_group_to_annotations(annotation.polygons, group)
                instance.points += _add_group_to_annotations(annotation.points, group)
                instance.tags += _add_group_to_annotations(annotation.tags, group)
                instance.polylines += _add_group_to_annotations(annotation.polylines, group)
                instance.captions += _add_group_to_annotations(annotation.captions, group)

            instance.has_image = images.get(annotation.event_id, False)
            instance.labels = instance.get_labels()
            result.append(instance)

        return await engine.save_all(result)

    @staticmethod
    async def delete_annotations(annotations: ImageAnnotations, group: Optional[str]):
        if not group:
            await engine.delete(annotations)
        else:
            new_annotations = ImageAnnotationsPatchSchema(
                tags=[], captions=[], detections=[], polygons=[], polylines=[], points=[])
            return await AnnotationsService.update_annotations(annotations, new_annotations, group)

    @staticmethod
    async def add_annotations_file(file: str,
                                   annotations_format: DatasetImportFormat,
                                   replace: bool,
                                   group: str,
                                   project_id: ObjectId):
        annotations = import_dataset(file, annotations_format, project_id)
        return await AnnotationsService.add_annotations_bulk(annotations, replace, group, project_id)

    @staticmethod
    async def get_stages_schema(project_id: ObjectId):
        result = {}
        project_labels = await ProjectService.get_project_labels(project_id)
        project_attributes = await ProjectService.get_project_attributes(project_id)

        for stage_id, stage_class in STAGES.items():
            try:
                result[stage_id] = stage_class.get_json_schema(
                    project_labels=project_labels, project_attributes=project_attributes)
            except Exception as e:
                raise e

        return result
