import os
from io import BytesIO
from urllib.parse import unquote_plus

import boto3
import pymongo
from PIL import Image

MONGO_CLIENT = pymongo.MongoClient(
    os.environ['MONGO_SRV_URI'],
    int(os.environ.get('MONGO_PORT', 27017))
)

MONGO_DB = MONGO_CLIENT[os.environ['MONGO_DATABASE']]

s3 = boto3.resource('s3')


def create_thumbnail(bucket_name, input_key, output_key, max_width, max_height):
    obj = s3.Object(
        bucket_name=bucket_name,
        key=input_key,
    )
    obj_body = obj.get()['Body'].read()

    img = Image.open(BytesIO(obj_body))
    img = img.resize((max_width, max_height), Image.ANTIALIAS)
    buffer = BytesIO()
    img.save(buffer, 'JPEG')
    buffer.seek(0)

    obj = s3.Object(bucket_name=bucket_name, key=output_key)
    obj.put(Body=buffer, ContentType='image/jpeg')


def remove_thumbnail(bucket_name, key):
    obj = s3.Object(bucket_name=bucket_name, key=key)
    obj.delete()


def update_database(event_id: str, value: bool):
    MONGO_DB.image_annotations.update_one({'event_id': event_id}, {'$set': {'has_image': value}})

    if value:
        MONGO_DB.images.insert_one({'event_id': event_id})
    else:
        MONGO_DB.images.delete_one({'event_id': event_id})


def main(event, context):
    for record in event['Records']:
        thumbnails_folder = os.environ['THUMBNAILS_FOLDER']
        max_width = int(os.environ['MAX_WIDTH'])
        max_height = int(os.environ['MAX_HEIGHT'])

        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        project_id, filename = key.split()[-2:]
        thumbnail_key = f'{thumbnails_folder}/{project_id}/{filename}'

        is_object_creation = event['eventName'].startswith('ObjectCreated')

        if is_object_creation:
            create_thumbnail(bucket, key, thumbnail_key, max_width, max_height)
        else:
            remove_thumbnail(bucket, thumbnail_key)

        update_database(filename, is_object_creation)
