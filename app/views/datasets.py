from typing import List

from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status
from odmantic import ObjectId

from app.schema import DatasetPostSchema, DatasetGetSortQuery, ImageAnnotationsPostSchema
from app.models import Dataset, ImageAnnotations, Label
from app.security import get_project_id
from app.config import Config
from app.services.datasets import DatasetService

router = InferringRouter()


@cbv(router)
class DatasetsView:
    project_id: ObjectId = Depends(get_project_id)

    @router.get("/dataset/{id}")
    async def get_dataset_by_id(self, id: ObjectId) -> Dataset:
        return await DatasetService.get_dataset_by_id(id, self.project_id)

    @router.get("/dataset/{id}/annotations")
    async def get_annotations_by_dataset_id(self, id: ObjectId) -> List[ImageAnnotations]:
        return await DatasetService.get_annotations_by_dataset_id(id, self.project_id)

    @router.post("/dataset/{id}/annotations")
    async def attach_annotations_to_dataset(self, id: ObjectId,
                                            annotations_ids: List[ObjectId] = [],
                                            annotations: List[ImageAnnotationsPostSchema] = []) -> List[ImageAnnotations]:
        if len(annotations) > Config.POST_BULK_LIMIT:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                                f'Payload too large. The maximum number of annotations to be'
                                f' added in a single request is {Config.POST_BULK_LIMIT}')

        return await DatasetService.attach_annotations_to_dataset(
            id, annotations_ids, annotations, self.project_id)

    @router.get("/dataset")
    async def get_datasets(self, name: DatasetGetSortQuery = None,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        return await DatasetService.get_datasets(self.project_id, name, sort)

    @router.post("/dataset")
    async def add_dataset(self, dataset: DatasetPostSchema) -> Dataset:
        return await DatasetService.create_dataset(dataset, self.project_id)

    @router.put("/dataset/{id}")
    async def add_dataset(self, id: ObjectId, dataset: DatasetPostSchema) -> Dataset:
        return await DatasetService.update_dataset(id, dataset, self.project_id)

    @router.delete("/dataset/{id}")
    async def delete_dataset(self, id: ObjectId) -> Dataset:
        return await DatasetService.delete_dataset(id, self.project_id)

    @router.get("/dataset/{id}/labels")
    async def get_dataset_labels(self, id: ObjectId) -> List[Label]:
        return await DatasetService.get_dataset_labels(id, self.project_id)
