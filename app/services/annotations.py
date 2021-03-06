from typing import List

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsQueryResult
from app.models import ImageAnnotations, Project, engine, Image
from app.core.importers import DatasetImportFormat, import_dataset
from app.core.query_engine.stages import STAGES, QueryStage, make_paginated_pipeline, QueryPipeline
from app.services.projects import ProjectService
from app.services.storage import StorageService


class AnnotationsService:
    @staticmethod
    async def get_annotations_by_id(annotations_id: ObjectId, project_id: ObjectId) -> ImageAnnotations:
        annotations = await engine.find_one(
            ImageAnnotations,
            (ImageAnnotations.id == annotations_id) & (ImageAnnotations.project_id == project_id))

        if annotations is None:
            raise HTTPException(404)

        return annotations

    @staticmethod
    async def get_annotations(page_size: int, page: int, project: Project) -> AnnotationsQueryResult:
        pipeline = [{'$match': {'project_id': project.id}}]
        pipeline = make_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)
        return AnnotationsQueryResult(data=result['data'], pagination=result['metadata'][0])

    @staticmethod
    async def run_annotations_pipeline(query: List[QueryStage],
                                       page_size: int, page: int,
                                       project: Project) -> AnnotationsQueryResult:
        pipeline = [{'$match': {'project_id': project.id}}]

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

        pipeline = make_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)

        for item in result['data']:
            if item.get(+ImageAnnotations.has_image):
                item['thumbnail_url'] = StorageService.create_presigned_get_url_for_thumbnail(
                    item['event_id'], project.id)
                item['image_url'] = StorageService.create_presigned_get_url_for_image(
                    item['event_id'], project.id)

        pipeline_obj = QueryPipeline(steps=query, project_id=project.id)
        pipeline_obj = await engine.save(pipeline_obj)

        pagination = result['metadata'][0] if result['metadata'] else {'page': 0, 'total': 0}
        data = result['data']

        return AnnotationsQueryResult(
            data=data, pagination=pagination, pipeline_id=pipeline_obj.id)

    @staticmethod
    async def add_annotations(annotation: ImageAnnotationsPostSchema,
                              replace: bool, project_id: ObjectId) -> ImageAnnotations:
        result = await AnnotationsService.add_annotations_bulk([annotation], replace, project_id)
        return result[0]

    @staticmethod
    async def add_annotations_bulk(annotations: List[ImageAnnotationsPostSchema],
                                   replace: bool,
                                   project_id: ObjectId) -> List[ImageAnnotations]:
        event_ids = [annotation.event_id for annotation in annotations]
        previous_instances = await engine.find(
            ImageAnnotations, ImageAnnotations.event_id.in_(event_ids))
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
                instance.detections = annotation.detections
                instance.polygons = annotation.polygons
                instance.points = annotation.points
                instance.tags = annotation.tags
                instance.polylines = annotation.polylines
            else:
                instance.detections += annotation.detections
                instance.polygons += annotation.polygons
                instance.points += annotation.points
                instance.tags += annotation.tags
                instance.polylines += annotation.polylines

            instance.has_image = images.get(annotation.event_id, False)
            instance.labels = instance.get_labels()
            result.append(instance)

        return await engine.save_all(result)

    @staticmethod
    async def update_annotations(annotations_id: ObjectId,
                                 annotation: ImageAnnotationsPostSchema,
                                 project_id: ObjectId) -> ImageAnnotations:
        instance = ImageAnnotations(id=annotations_id, project_id=project_id, **annotation.dict())
        return await engine.save(instance)

    @staticmethod
    async def delete_annotations(id: ObjectId, project_id: ObjectId):
        annotations = await AnnotationsService.get_annotations_by_id(id, project_id)
        await engine.delete(annotations)

    @staticmethod
    async def add_annotations_file(file: str,
                                   annotations_format: DatasetImportFormat,
                                   replace: bool,
                                   project_id: ObjectId):
        annotations = import_dataset(file, annotations_format, project_id)
        return await AnnotationsService.add_annotations_bulk(annotations, replace, project_id)

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
