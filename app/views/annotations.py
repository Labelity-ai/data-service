from typing import List, Dict
from pathlib import Path
import tempfile

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, File, UploadFile
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, AnnotationsQueryResult
from app.models import ImageAnnotations, Project
from app.security import get_project
from app.config import Config
from app.services.annotations import AnnotationsService
from app.core.importers import DatasetImportFormat
from app.core.query_engine.stages import QueryStage


router = InferringRouter(
    tags=["annotations"],
)


@cbv(router)
class AnnotationsView:
    project: Project = Depends(get_project)

    @router.get("/annotations/{id}")
    async def get_annotations_by_id(self, id: ObjectId) -> ImageAnnotations:
        return await AnnotationsService.get_annotations_by_id(id, self.project.id)

    @router.post("/annotations/pipeline")
    async def run_annotations_pipeline(self, query: List[QueryStage], page=0, page_size=10) -> AnnotationsQueryResult:
        return await AnnotationsService.run_annotations_pipeline(
            query=query, page_size=page_size, page=page, project=self.project)

    @router.post("/annotations")
    async def add_annotations(self, annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        return await AnnotationsService.add_annotations(annotation, self.project.id)

    @router.post("/annotations_bulk")
    async def add_annotations_bulk(self, annotations: List[ImageAnnotationsPostSchema]) -> List[ImageAnnotations]:
        if len(annotations) > Config.POST_BULK_LIMIT:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                                f'Payload too large. The maximum number of annotations to be'
                                f' added in a single request is {Config.POST_BULK_LIMIT}')

        return await AnnotationsService.add_annotations_bulk(annotations, self.project.id)

    @router.post("/annotations_file")
    async def add_annotations_file(self,
                                   annotations_format: DatasetImportFormat,
                                   file: UploadFile = File(...)) -> List[ImageAnnotations]:
        if file.content_type not in ['application/xml', 'application/json', 'text/xml']:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                'File should be one of the supported mime types (xml or json)')

        file_content = await file.read()

        with tempfile.TemporaryDirectory() as temp_dir:
            with open(f'{temp_dir}/{file.filename}', 'wb') as temp_file:
                temp_file.write(file_content)
                temp_file.flush()
                print(temp_file.name)
                return await AnnotationsService.add_annotations_file(
                    Path(temp_file.name), annotations_format, self.project.id)

    @router.patch("/annotations/{id}")
    async def update_annotations(self, id: ObjectId,
                                 annotation: ImageAnnotationsPostSchema) -> ImageAnnotations:
        return await AnnotationsService.update_annotations(id, annotation, self.project_id)

    @router.delete("/annotations/{id}")
    async def delete_annotations(self, id: ObjectId) -> APIMessage:
        await AnnotationsService.delete_annotations(id, self.project.id)
        return APIMessage(detail=f"Deleted annotations {id}")

    @router.get("/annotations/meta/stages")
    async def get_annotations_by_id(self) -> Dict[str, dict]:
        return await AnnotationsService.get_stages_schema(self.project.id)
