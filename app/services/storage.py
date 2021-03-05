import os
import boto3

from app.config import Config
from app.models import ObjectId

s3_client = boto3.client('s3')


class StorageService:
    @staticmethod
    def create_presigned_post_url_for_image(image_name: str, content_type: str, project_id: ObjectId) -> dict:
        extension = content_type.split('/')[-1]
        filename = image_name if image_name.endswith(f'.{extension}') else f'{image_name}.{extension}'

        return s3_client.generate_presigned_post(
            Config.IMAGE_STORAGE_BUCKET,
            f'raw/{project_id}/{filename}',
            Conditions=[{'Content-Type': content_type}],
            ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
        )

    @staticmethod
    def create_presigned_post_url_for_video(video_name: str, content_type: str,
                                            start_sec: int, end_sec: int, fps: float,
                                            project_id: ObjectId) -> dict:
        extension = content_type.split('/')[-1]
        video_name = os.path.splitext(video_name)[0]
        filename = f'{video_name}__{start_sec}_{end_sec}_{fps}.{extension}'

        return s3_client.generate_presigned_post(
            Config.IMAGE_STORAGE_BUCKET,
            f'{Config.VIDEOS_FOLDER}/{project_id}/{filename}',
            Conditions=[{'Content-Type': content_type}],
            ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
        )

    @staticmethod
    def create_presigned_get_url_for_thumbnail(event_id: str, project_id: ObjectId) -> str:
        return s3_client.generate_presigned_url(
            'get_object',
            ExpiresIn=Config.SIGNED_GET_THUMBNAIL_URL_EXPIRATION,
            Params={
                'Bucket': Config.IMAGE_STORAGE_BUCKET,
                'Key': f'{Config.THUMBNAILS_FOLDER}/{project_id}/{event_id}'
            },
        )

    @staticmethod
    def create_presigned_get_url_for_image(event_id: str, project_id: ObjectId) -> str:
        return s3_client.generate_presigned_url(
            'get_object',
            ExpiresIn=Config.SIGNED_GET_IMAGE_URL_EXPIRATION,
            Params={
                'Bucket': Config.IMAGE_STORAGE_BUCKET,
                'Key': f'{Config.RAW_IMAGES_FOLDER}/{project_id}/{event_id}'
            },
        )

    @staticmethod
    def create_presigned_get_url_for_object_download(key: str) -> str:
        return s3_client.generate_presigned_url(
            'get_object',
            ExpiresIn=Config.SIGNED_GET_OBJECT_URL_EXPIRATION,
            Params={
                'Bucket': Config.IMAGE_STORAGE_BUCKET,
                'Key': key,
            },
        )
