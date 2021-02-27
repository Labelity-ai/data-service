from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends, HTTPException, status

from app.models import Project
from app.security import get_project
from app.services.storage import StorageService
from app.config import Config

router = InferringRouter(tags=["storage"])


@cbv(router)
class StorageView:
    project: Project = Depends(get_project)

    @router.post("/storage/image")
    async def get_signed_url_for_image_uploading(self, image_id: str, mime_type: str) -> dict:
        supported_mime_types = ['image/png', 'image/jpeg', 'image/bmp', 'image/webp']

        if mime_type not in supported_mime_types:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f'mime_type param should be one of {supported_mime_types}')

        return StorageService.create_presigned_post_url_for_image(
            image_id, mime_type, self.project.id)

    @router.post("/storage/video")
    async def get_signed_url_for_video_uploading(self, video_name: str, start_sec: int,
                                                 end_sec: int, fps: float, mime_type: str) -> dict:
        supported_mime_types = ['video/mp4']

        if mime_type not in supported_mime_types:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f'mime_type param should be one of {supported_mime_types}')

        if not fps or fps > Config.VIDEO_FPS_LIMIT:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f'fps param should be higher than 0 and lower than {Config.VIDEO_FPS_LIMIT}')

        return StorageService.create_presigned_post_url_for_video(
            video_name, mime_type, start_sec, end_sec, fps, self.project.id)

