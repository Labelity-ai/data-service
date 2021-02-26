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
        extension = content_type.split('/')[-1]
        filename = image_name if image_name.endswith(f'.{extension}') else f'{image_name}.{extension}'

        return s3_client.generate_presigned_post(
            Config.IMAGE_STORAGE_BUCKET,
            f'{folder}/{project_id}/{filename}',
            Conditions=[{'Content-Type': content_type}],
            ExpiresIn=Config.SIGNED_POST_URL_EXPIRATION
        )
