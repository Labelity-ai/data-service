from typing import List
import os
from fastapi import HTTPException
import aioboto3

from app.config import Config
from app.models import ObjectId, Image, ImageAnnotations, engine
from app.schema import ImageData


class StorageService:
    @staticmethod
    async def create_presigned_post_url_for_image(image_name: str, content_type: str, project_id: ObjectId) -> dict:
        extension = content_type.split('/')[-1]
        filename = image_name if image_name.endswith(f'.{extension}') else f'{image_name}.{extension}'

        async with aioboto3.client("s3") as s3_client:
            return await s3_client.generate_presigned_post(
                Config.IMAGE_STORAGE_BUCKET,
                f'{Config.RAW_IMAGES_FOLDER}/{project_id}/{filename}',
                Conditions=[{'Content-Type': content_type}],
                ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
            )

    @staticmethod
    async def create_presigned_post_url_for_video(video_name: str, content_type: str,
                                                  start_sec: int, end_sec: int, fps: float,
                                                  project_id: ObjectId) -> dict:
        extension = content_type.split('/')[-1]
        video_name = os.path.splitext(video_name)[0]
        filename = f'{video_name}__{start_sec}_{end_sec}_{fps}.{extension}'

        async with aioboto3.client("s3") as s3_client:
            return await s3_client.generate_presigned_post(
                Config.IMAGE_STORAGE_BUCKET,
                f'{Config.VIDEOS_FOLDER}/{project_id}/{filename}',
                Conditions=[{'Content-Type': content_type}],
                ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
            )

    @staticmethod
    async def create_presigned_get_url_for_thumbnail(event_id: str, project_id: ObjectId) -> str:
        async with aioboto3.client("s3") as s3_client:
            return await s3_client.generate_presigned_url(
                'get_object',
                ExpiresIn=Config.SIGNED_GET_THUMBNAIL_URL_EXPIRATION,
                Params={
                    'Bucket': Config.IMAGE_STORAGE_BUCKET,
                    'Key': f'{Config.THUMBNAILS_FOLDER}/{project_id}/{event_id}'
                },
            )

    @staticmethod
    async def create_presigned_get_url_for_image(event_id: str, project_id: ObjectId) -> str:
        async with aioboto3.client("s3") as s3_client:
            return await s3_client.generate_presigned_url(
                'get_object',
                ExpiresIn=Config.SIGNED_GET_IMAGE_URL_EXPIRATION,
                Params={
                    'Bucket': Config.IMAGE_STORAGE_BUCKET,
                    'Key': f'{Config.RAW_IMAGES_FOLDER}/{project_id}/{event_id}'
                },
            )

    @staticmethod
    async def create_presigned_get_url_for_object_download(key: str) -> str:
        async with aioboto3.client("s3") as s3_client:
            return await s3_client.generate_presigned_url(
                'get_object',
                ExpiresIn=Config.SIGNED_GET_OBJECT_URL_EXPIRATION,
                Params={
                    'Bucket': Config.DATASET_ARTIFACTS_BUCKET,
                    'Key': '/'.join(key.split('/')[1:]),
                },
            )

    @staticmethod
    async def delete_image(event_id: str, project_id: ObjectId):
        # TODO: Remove str wrapper
        image = await engine.find_one(Image, Image.project_id == str(project_id), Image.event_id == event_id)
        annotations = await engine.find_one(
            ImageAnnotations, ImageAnnotations.project_id == project_id,
            ImageAnnotations.event_id == event_id)

        if not image:
            raise HTTPException(404)

        async with aioboto3.client("s3") as s3_client:
            await s3_client.delete_object(
                Bucket=Config.IMAGE_STORAGE_BUCKET,
                Key=f'{Config.RAW_IMAGES_FOLDER}/{project_id}/{event_id}')

        await engine.delete(image)

        if annotations:
            annotations.has_image = False
            await engine.save(annotations)

    @staticmethod
    async def get_images(event_id: str, page: int, page_size: int, project_id: ObjectId) -> List[ImageData]:
        if event_id:
            image = await engine.find_one(Image, Image.project_id == project_id, Image.event_id == event_id)
            return [image] if image else []

        # TODO: Remove str wrapper
        images = await engine.find(Image, Image.project_id == str(project_id),
                                   sort=Image.event_id,
                                   skip=page * page_size,
                                   limit=page_size)

        result = []

        for image in images:
            thumbnail_url = await StorageService.create_presigned_get_url_for_thumbnail(
                image.event_id, project_id)
            original_url = await StorageService.create_presigned_get_url_for_image(
                image.event_id, project_id)

            result.append(ImageData(
                event_id=image.event_id,
                width=image.width,
                height=image.height,
                thumbnail_url=thumbnail_url,
                original_url=original_url
            ))

        return result
