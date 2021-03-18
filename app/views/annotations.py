from typing import List, Dict, Union
import tempfile

from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status, File, UploadFile

from app.schema import ImageAnnotationsPostSchema, AnnotationsQueryResult, \
    ImageAnnotationsPutSchema, ImageAnnotationsPatchSchema
from app.models import ImageAnnotations, Project
from app.security import get_project
from app.config import Config
from app.services.annotations import AnnotationsService, AnnotationSortDirection, AnnotationSortField
from app.services.storage import StorageService
from app.core.importers import DatasetImportFormat
from app.core.query_engine.stages import QueryStage

router = InferringRouter(
    tags=["annotations"],
)


@cbv(router)
class AnnotationsView:
    project: Project = Depends(get_project)

    @router.get("/annotations/")
    async def get_annotations(self, page: int = 0,
                              page_size: int = 10,
                              only_with_images: bool = True,
                              sort_field: AnnotationSortField = AnnotationSortField.IMAGE_NAME,
                              sort_direction: AnnotationSortDirection = AnnotationSortDirection.DESCENDING,
                              ) -> AnnotationsQueryResult:
        return await AnnotationsService.get_annotations(
            page_size=page_size,
            page=page,
            project=self.project,
            only_with_images=only_with_images,
            sort_field=sort_field,
            sort_direction=sort_direction)

    @router.post("/annotations/pipeline")
    async def run_annotations_pipeline(self, query: List[QueryStage],
                                       page: int = 0, page_size: int = 10) -> AnnotationsQueryResult:
        return await AnnotationsService.run_annotations_pipeline(
            query=query, page_size=int(page_size), page=int(page), project=self.project)

    @router.post("/annotations")
    async def add_annotations(self, annotation: ImageAnnotationsPostSchema,
                              replace: bool = True, group='ground_truth') -> ImageAnnotations:
        return await AnnotationsService.add_annotations(annotation, replace, group, self.project.id)

    @router.post("/annotations_bulk")
    async def add_annotations_bulk(self, annotations: List[ImageAnnotationsPostSchema],
                                   group: str = 'ground_truth',
                                   replace: bool = True):
        if len(annotations) > Config.POST_BULK_LIMIT:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                                f'Payload too large. The maximum number of annotations to be'
                                f' added in a single request is {Config.POST_BULK_LIMIT}')

        await AnnotationsService.add_annotations_bulk(annotations, replace, group, self.project.id)

    @router.post("/annotations_file")
    async def add_annotations_file(self,
                                   annotations_format: DatasetImportFormat,
                                   replace: bool,
                                   file: UploadFile = File(...),
                                   group: str = 'group_truth'):
        if file.content_type not in ['application/xml', 'application/json', 'text/xml']:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                'File should be one of the supported mime types (xml or json)')

        file_content = await file.read()

        with tempfile.TemporaryDirectory() as temp_dir:
            with open(f'{temp_dir}/{file.filename}', 'wb') as temp_file:
                temp_file.write(file_content)
                temp_file.flush()
                print(temp_file.name)
                await AnnotationsService.add_annotations_file(
                    temp_file.name, annotations_format, replace, group, self.project.id)

    @router.get("/annotations/{event_id}")
    async def get_annotations_by_event_id(self, event_id: str) -> ImageAnnotations:
        return await AnnotationsService.get_annotations_by_event_id(event_id, self.project.id)

    @router.delete("/annotations/{event_id}")
    async def delete_annotations(self, event_id: str,
                                 group: str = None,
                                 delete_image: bool = False) -> Union[APIMessage, ImageAnnotations]:
        if delete_image and group:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                'group and delete_image parameters are incompatible')

        instance = await self.get_annotations_by_event_id(event_id)
        result = await AnnotationsService.delete_annotations(instance, group)

        if delete_image:
            await StorageService.delete_image(event_id, self.project.id)

        return result if result else APIMessage(detail=f"Deleted annotations for {event_id}")

    @router.put("/annotations/{event_id}")
    async def replace_annotations(self, event_id: str,
                                  body: ImageAnnotationsPutSchema,
                                  group: str = None) -> ImageAnnotations:
        instance = await self.get_annotations_by_event_id(event_id)
        return await AnnotationsService.update_annotations(instance, body, group)

    @router.patch("/annotations/{event_id}")
    async def update_annotations(self, event_id: str,
                                 body: ImageAnnotationsPatchSchema,
                                 group: str = None) -> ImageAnnotations:
        instance = await self.get_annotations_by_event_id(event_id)
        return await AnnotationsService.update_annotations(instance, body, group)

    @router.get("/annotations/meta/stages")
    async def get_annotations_stages(self) -> Dict[str, dict]:
        return await AnnotationsService.get_stages_schema(self.project.id)

    @router.get("/annotations_file/formats")
    async def get_export_formats(self) -> List[str]:
        return [x.value for x in DatasetImportFormat]
