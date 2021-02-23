from enum import Enum
import boto3

from app.config import Config
from app.models import ObjectId

s3_client = boto3.client('s3')


class Filetype(Enum):
    IMAGE = 'image'
    VIDEO = 'video'


class StorageService:
    @staticmethod
    def create_presigned_post_url(image_name: str, content_type: str,
                                        filetype: Filetype, project_id: ObjectId) -> dict:
        folder = 'raw' if filetype == Filetype.IMAGE else 'videos'

        return s3_client.generate_presigned_post(
            Config.IMAGE_STORAGE_BUCKET,
            f'{project_id}/{folder}/{image_name}',
            Conditions=[{'Content-Type': content_type}],
            ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
        )
