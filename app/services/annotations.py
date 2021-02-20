from typing import List
from pathlib import Path

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsQueryResult
from app.models import ImageAnnotations, Project, engine
from app.core.importers import DatasetImportFormat, import_dataset
from app.core.query_engine.stages import STAGES, QueryStage, make_paginated_pipeline
from app.services.projects import ProjectService


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
    async def run_annotations_pipeline(query: List[QueryStage],
                                       page_size: int, page: int,
                                       project: Project) -> AnnotationsQueryResult:
        pipeline = [{'$match': {'project_id': project.id}}]

        project_labels = await ProjectService.get_project_labels(project.id)
        project_attributes = await ProjectService.get_project_attributes(project.id)

        for step in query:
            try:
                stage = STAGES[step.stage](**step.parameters)
                stage.validate_stage(project_labels=project_labels, project_attributes=project_attributes)
                pipeline.extend(stage.to_mongo())
            except ValueError as error:
                print(error)
                raise HTTPException(400, detail=str(error))

        pipeline = make_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)
        print(result)
        return AnnotationsQueryResult(data=result['data'], metadata=result['metadata'][0])

    @staticmethod
    async def add_annotations(annotation: ImageAnnotationsPostSchema, project_id: ObjectId) -> ImageAnnotations:
        result = await AnnotationsService.add_annotations_bulk([annotation], project_id)
        return result[0]

    @staticmethod
    async def add_annotations_bulk(annotations: List[ImageAnnotationsPostSchema],
                                   project_id: ObjectId) -> List[ImageAnnotations]:
        instances = [
            ImageAnnotations(**annotation.dict(), project_id=project_id)
            for annotation in annotations
        ]
        return await engine.save_all(instances)

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
    async def add_annotations_file(file: Path,
                                   annotations_format: DatasetImportFormat,
                                   project_id: ObjectId):
        annotations = import_dataset(file, annotations_format, project_id)
        return await engine.save_all(annotations)

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
