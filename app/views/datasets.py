from typing import List

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, Response
from odmantic import ObjectId

from app.schema import DatasetPostSchema, DatasetGetSortQuery, \
    DatasetToken, ImageAnnotationsData, DatasetPatchSchema, JobId
from app.models import Dataset, Label, Project, FastToken
from app.security import get_project, get_dataset_token
from app.services.datasets import DatasetService, DatasetExportFormat
from app.core.tracing import traced

router = InferringRouter(
    tags=["datasets"],
)


@traced
class DatasetsViewBase:
    @staticmethod
    async def get_dataset_by_id(id: ObjectId, project_id) -> Dataset:
        return await DatasetService.get_dataset_by_id(id, project_id)

    @staticmethod
    async def get_annotations_by_dataset_id(id: ObjectId, project_id: ObjectId) -> List[ImageAnnotationsData]:
        return await DatasetService.get_annotations_by_dataset_id(id, project_id)

    @staticmethod
    async def get_datasets(project_id: ObjectId, name: str = None,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        return await DatasetService.get_datasets(project_id, name, sort=sort)

    @staticmethod
    async def create_dataset(dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        return await DatasetService.create_dataset(dataset, project_id)

    @staticmethod
    async def replace_dataset(id: ObjectId, body: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        dataset = await DatasetsViewBase.get_dataset_by_id(id, project_id)
        return await DatasetService.update_dataset(dataset, body)

    @staticmethod
    async def update_dataset(id: ObjectId, body: DatasetPatchSchema, project_id: ObjectId) -> Dataset:
        dataset = await DatasetsViewBase.get_dataset_by_id(id, project_id)
        return await DatasetService.update_dataset(dataset, body)

    @staticmethod
    async def delete_dataset(id: ObjectId, project_id: ObjectId):
        await DatasetService.delete_dataset(id, project_id)
        return APIMessage(detail=f"Deleted dataset {id}")

    @staticmethod
    async def get_dataset_labels(id: ObjectId, project_id: ObjectId) -> List[Label]:
        dataset = await DatasetsViewBase.get_dataset_by_id(id, project_id)
        return await DatasetService.get_dataset_labels(dataset)

    @staticmethod
    async def create_dataset_access_token(id: ObjectId, project_id: ObjectId) -> List[Label]:
        dataset = await DatasetsViewBase.get_dataset_by_id(id, project_id)
        return await DatasetService.create_access_token(dataset)

    @staticmethod
    async def download_dataset(id: ObjectId, project_id: ObjectId, format: DatasetExportFormat) -> JobId:
        dataset = await DatasetService.get_dataset_by_id(id, project_id)
        job_id = await DatasetService.download_dataset(dataset, format)
        return JobId(job_id=job_id)

    @staticmethod
    async def get_dataset_download_url(job_id: str) -> APIMessage:
        url = await DatasetService.get_dataset_download_url(job_id)
        return APIMessage(detail=url)

    @staticmethod
    def get_export_formats() -> List[str]:
        return [x.value for x in DatasetExportFormat]


@cbv(router)
@traced
class DatasetsView:
    project: Project = Depends(get_project)

    @router.get("/dataset/{id}")
    async def get_dataset_by_id(self, id: ObjectId) -> Dataset:
        return await DatasetsViewBase.get_dataset_by_id(id, self.project.id)

    @router.get("/dataset/{id}/annotations")
    async def get_annotations_by_dataset_id(self, id: ObjectId) -> List[ImageAnnotationsData]:
        return await DatasetsViewBase.get_annotations_by_dataset_id(id, self.project.id)

    @router.get("/dataset")
    async def get_datasets(self, name: str = None, include_all_revisions: bool = False,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        return await DatasetService.get_datasets(
            self.project.id, name, include_all_revisions, sort)

    @router.post("/dataset")
    async def create_dataset(self, dataset: DatasetPostSchema) -> Dataset:
        return await DatasetsViewBase.create_dataset(dataset, self.project.id)

    @router.put("/dataset/{id}")
    async def replace_dataset(self, id: ObjectId, dataset: DatasetPostSchema) -> Dataset:
        return await DatasetsViewBase.replace_dataset(id, dataset, self.project.id)

    @router.patch("/dataset/{id}")
    async def update_dataset(self, id: ObjectId, dataset: DatasetPatchSchema) -> Dataset:
        return await DatasetsViewBase.update_dataset(id, dataset, self.project.id)

    @router.delete("/dataset/{id}")
    async def delete_dataset(self, id: ObjectId):
        return await DatasetsViewBase.delete_dataset(id, self.project.id)

    @router.get("/dataset/{id}/labels")
    async def get_dataset_labels(self, id: ObjectId) -> List[Label]:
        return await DatasetsViewBase.get_dataset_labels(id, self.project.id)

    @router.post("/dataset/{id}/token")
    async def create_dataset_access_token(self, id: ObjectId) -> DatasetToken:
        dataset = await self.get_dataset_by_id(id)
        return await DatasetService.create_access_token(dataset)

    @router.get("/dataset/{id}/download", status_code=202)
    async def download_dataset(self, id: ObjectId, format: DatasetExportFormat) -> JobId:
        return await DatasetsViewBase.download_dataset(id, self.project.id, format)

    @router.get("/dataset/{id}/download_url")
    async def get_download_dataset_url(self, job_id: str) -> APIMessage:
        return await DatasetsViewBase.get_dataset_download_url(job_id)

    @router.get("/dataset/{id}/revisions")
    async def get_dataset_revisions(self, id: ObjectId) -> List[Dataset]:
        dataset = await self.get_dataset_by_id(id)
        return await DatasetService.get_dataset_revisions(dataset)

    @router.get("/meta/dataset/formats")
    def get_export_formats(self) -> List[str]:
        return DatasetsViewBase.get_export_formats()


@cbv(router)
@traced
class DatasetsSharedView:
    dataset_token: FastToken = Depends(get_dataset_token)

    @router.get("/dataset_shared")
    async def get_dataset(self) -> Dataset:
        return await DatasetsViewBase.get_dataset_by_id(
            self.dataset_token.dataset_id, self.dataset_token.project_id)

    @router.get("/dataset_shared/annotations")
    async def get_annotations_by_dataset_id(self) -> List[ImageAnnotationsData]:
        return await DatasetsViewBase.get_annotations_by_dataset_id(
            self.dataset_token.dataset_id, self.dataset_token.project_id
        )

    @router.get("/dataset_shared/labels")
    async def get_dataset_labels(self) -> List[Label]:
        return await DatasetsViewBase.get_dataset_labels(
            self.dataset_token.dataset_id, self.dataset_token.project_id
        )

    @router.get("/dataset_shared/download")
    async def download_dataset(self, format: DatasetExportFormat, response: Response) -> JobId:
        return await DatasetsViewBase.download_dataset(
            self.dataset_token.dataset_id, self.dataset_token.project_id, format, response
        )

    @router.get("/dataset/{id}/download_url")
    async def download_dataset(self, job_id: str) -> APIMessage:
        return await DatasetsViewBase.get_dataset_download_url(job_id)

    @router.get("/meta/dataset_shared/formats")
    def get_export_formats(self) -> List[str]:
        return DatasetsViewBase.get_export_formats()
