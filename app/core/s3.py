import aioboto3
from app.config import Config

session = aioboto3.Session()


def get_s3_client():
    return session.client('s3', endpoint_url=Config.AWS_ENDPOINT_URL)
