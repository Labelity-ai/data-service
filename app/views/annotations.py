from typing import List
from pathlib import Path
import tempfile

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, File, UploadFile
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsQuery
from app.models import ImageAnnotations
from app.security import get_project_id
from app.config import Config
from app.services.annotations import AnnotationsService
from app.utils import json_loads
from app.core.importers import DatasetImportFormat


router = InferringRouter(
    tags=["annotations"],
)


@cbv(router)
class AnnotationsView:
    project_id: ObjectId = Depends(get_project_id)

    @router.get("/annotations/{id}")
    async def get_annotations_by_id(self, id: ObjectId) -> ImageAnnotations:
        return await AnnotationsService.get_annotations_by_id(id, self.project_id)

    @router.get("/annotations")
    async def get_annotations(self, query: str) -> List[ImageAnnotations]:
        try:
            query_json = json_loads(query)
        except Exception:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, 'The query field should be a valid JSON')

        query_obj = AnnotationsQuery(**query_json)
        return await AnnotationsService.get_annotations(query_obj, self.project_id)

    @router.post("/annotations")
    async def add_annotations(self, annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        return await AnnotationsService.add_annotations(annotation, self.project_id)

    @router.post("/annotations_bulk")
    async def add_annotations_bulk(self, annotations: List[ImageAnnotationsPostSchema]) -> List[ImageAnnotations]:
        if len(annotations) > Config.POST_BULK_LIMIT:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                                f'Payload too large. The maximum number of annotations to be'
                                f' added in a single request is {Config.POST_BULK_LIMIT}')

        return await AnnotationsService.add_annotations_bulk(annotations, self.project_id)

    @router.post("/annotations_file")
    async def add_annotations_file(self,
                                   annotations_format: DatasetImportFormat,
                                   file: UploadFile = File(...)) -> List[ImageAnnotations]:
        if file.content_type not in ['application/xml', 'application/json']:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                'File should be one of the supported mime types (xml or json)')

        file_content = await file.read()

        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(file_content)
            temp_file.flush()
            return await AnnotationsService.add_annotations_file(
                Path(temp_file.name), annotations_format, self.project_id)

    @router.patch("/annotations/{id}")
    async def update_annotations(self, id: ObjectId,
                                 annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        return await AnnotationsService.update_annotations(id, annotation, self.project_id)

    @router.delete("/annotations/{id}")
    async def delete_annotations(self, id: ObjectId) -> APIMessage:
        await AnnotationsService.delete_annotations(id, self.project_id)
        return APIMessage(detail=f"Deleted annotations {id}")
