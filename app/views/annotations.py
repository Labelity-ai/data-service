from typing import List

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, UploadFile, File
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsFormat
from app.models import ImageAnnotations, engine
from app.security import get_project_id
from app.config import Config


router = InferringRouter()


@cbv(router)
class AnnotationsView:
    project_id: ObjectId = Depends(get_project_id)

    @router.get("/annotations/{id}")
    async def get_annotations_by_id(self, id: ObjectId) -> ImageAnnotations:
        annotations = await engine.find_one(
            ImageAnnotations,
            (ImageAnnotations.id == id) & (ImageAnnotations.project_id == self.project_id))
        if annotations is None:
            raise HTTPException(404)
        return annotations

    @router.get("/annotations")
    async def get_annotations(self, event_id: str) -> ImageAnnotations:
        return await engine.find_one(
            ImageAnnotations,
            (ImageAnnotations.event_id == event_id) & (ImageAnnotations.project_id == self.project_id))

    @router.post("/annotations")
    async def add_annotations(self, annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        instance = ImageAnnotations(**annotation.dict(), project_id=self.project_id)
        return await engine.save(instance)

    @router.post("/annotations_bulk")
    async def add_annotations_bulk(self, annotations: List[ImageAnnotationsPostSchema]) -> List[ImageAnnotations]:
        instances = [ImageAnnotations(**annotation.dict(), project_id=self.project_id)
                     for annotation in annotations]

        if len(annotations) > Config.POST_BULK_LIMIT:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                                f'Payload too large. The maximum number of annotations to be'
                                f' added in a single request is {Config.POST_BULK_LIMIT}')

        return await engine.save_all(instances)

    @router.post("/annotations_file")
    async def add_annotations_file(self, format: AnnotationsFormat,
                                   file: UploadFile = File(...)) -> List[ImageAnnotations]:
        # TODO
        return []

    @router.patch("/annotations/{id}")
    async def update_annotations(self, id: ObjectId,
                                 annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        instance = ImageAnnotations(id=id, project_id=self.project_id, **annotation.dict())
        return await engine.save(instance)

    @router.delete("/annotations/{id}")
    async def delete_annotations(self, id: ObjectId) -> APIMessage:
        annotations = await engine.find_one(
            ImageAnnotations,
            (ImageAnnotations.id == id) & (ImageAnnotations.project_id == self.project_id))

        if annotations is None:
            raise HTTPException(404)

        await engine.delete(annotations)

        return APIMessage(detail=f"Deleted annotations {id}")
