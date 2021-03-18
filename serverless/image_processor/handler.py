import os
from io import BytesIO
from urllib.parse import unquote_plus

import boto3
import s3fs
import pymongo
import bson
from PIL import Image

MONGO_CLIENT = pymongo.MongoClient(
    os.environ['MONGO_URI'],
    int(os.environ.get('MONGO_PORT', 27017))
)

MONGO_DB = MONGO_CLIENT[os.environ['MONGO_DATABASE']]

s3 = boto3.resource('s3')
s3_fs = s3fs.S3FileSystem()


def create_thumbnail(bucket_name, input_key, output_key, max_width, max_height):
    obj = s3.Object(
        bucket_name=bucket_name,
        key=input_key,
    )
    obj_body = obj.get()['Body'].read()

    img = Image.open(BytesIO(obj_body))
    width, height = img.size
    img.thumbnail((max_width, max_height))
    buffer = BytesIO()
    img.save(buffer, 'JPEG')
    buffer.seek(0)

    obj = s3.Object(bucket_name=bucket_name, key=output_key)
    obj.put(Body=buffer, ContentType='image/jpeg')
    return width, height


def remove_thumbnail(bucket_name, key):
    obj = s3.Object(bucket_name=bucket_name, key=key)
    obj.delete()


def update_database(event_id: str, value: bool, project_id: str, width: int, height: int):
    MONGO_DB.image_annotations.update_one({'event_id': event_id}, {'$set': {'has_image': value}})

    if value:
        MONGO_DB.image.insert_one({
            'event_id': event_id,
            'width': width,
            'height': height,
            'project_id': bson.ObjectId(project_id.strip()),
        })
    else:
        MONGO_DB.image.delete_one({'event_id': event_id})


def main(event, context):
    for record in event['Records']:
        thumbnails_folder = os.environ['THUMBNAILS_FOLDER']
        max_width = int(os.environ['THUMBNAILS_MAX_WIDTH'])
        max_height = int(os.environ['THUMBNAILS_MAX_HEIGHT'])

        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        project_id, filename = key.split('/')[-2:]
        thumbnail_key = f'{thumbnails_folder}/{project_id}/{filename}'
        print('Processing', project_id, filename)

        is_object_creation = record['eventName'].startswith('ObjectCreated')

        if is_object_creation:
            width, height = create_thumbnail(bucket, key, thumbnail_key, max_width, max_height)
        else:
            remove_thumbnail(bucket, thumbnail_key)
            width = None
            height = None

        update_database(filename, is_object_creation, project_id, width, height)


if __name__ == '__main__':
    # This will only run on local development
    keys = s3_fs.glob(f'{os.environ["IMAGES_BUCKET"]}/raw/**.*')
    records = []

    for key in keys:
        records.append({
            'eventName': 'ObjectCreated:PUT',
            "s3": {
                "bucket": {"name": os.environ['IMAGES_BUCKET'].strip()},
                "object": {"key": '/'.join(key.split('/')[1:])}
            }
        })

    event = {"Records": records}
    main(event, None)
