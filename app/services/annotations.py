from typing import List
from pathlib import Path

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsQuery
from app.models import ImageAnnotations, engine
from app.core.importers import DatasetImportFormat, import_dataset


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
    async def get_annotations(query: AnnotationsQuery, project_id: ObjectId) -> List[ImageAnnotations]:
        # TODO
        return []

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
