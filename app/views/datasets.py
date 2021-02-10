from typing import List

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, UploadFile, File
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsFormat
from app.models import Dataset, engine, ImageAnnotations
from app.security import get_project_id
from app.config import Config


router = InferringRouter()


@cbv(router)
class DatasetsView:
    project_id: ObjectId = Depends(get_project_id)

    @router.get("/dataset/{id}")
    async def get_dataset_by_id(self, id: ObjectId) -> Dataset:
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == id) & (Dataset.project_id == self.project_id))
        if dataset is None:
            raise HTTPException(404)
        return dataset

    @router.get("/dataset/{id}/annotations")
    async def get_annotations_by_dataset_id(self, id: ObjectId) -> List[ImageAnnotations]:
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == id) & (Dataset.project_id == self.project_id))

        if dataset is None:
            raise HTTPException(404)

        return await engine.find(ImageAnnotations, ImageAnnotations.id.in_(dataset.annotations))

    @router.get("/dataset")
    async def get_datasets(self, name: str) -> List[Dataset]:
        return await engine.find(
            Dataset, (Dataset.name == name) & (Dataset.project_id == self.project_id))

    @router.post("/dataset")
    async def add_annotations(self, annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        instance = ImageAnnotations(**annotation.dict(), project_id=self.project_id)
        return await engine.save(instance)

