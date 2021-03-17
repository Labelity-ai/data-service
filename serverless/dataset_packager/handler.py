import os
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED

import s3fs
import cloudpickle
from datumaro.components.dataset import Dataset, DatasetItem
from datumaro.util.image import Image
from urllib.parse import unquote_plus


s3_fs = s3fs.S3FileSystem()


def zip_dir(path, zip_handler):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip_handler.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))


def create_dataset_zip(dataset: Dataset, format: str, images_bucket, project_id,
                       dataset_id, zip_name, output_bucket, output_folder):
    with tempfile.TemporaryDirectory() as tmpdir:
        for row in dataset:
            item: DatasetItem = row

            if not item.has_image:
                continue

            image_filename = item.image.path.split('/')[-1]
            image_path = f'{tmpdir}/{image_filename}'
            print(f'{images_bucket}/raw/{project_id}/{image_filename}', image_path)
            s3_fs.get(f'{images_bucket}/raw/{project_id}/{image_filename}', image_path)
            item.image = Image(path=image_path, size=item.image.size)

        dataset_folder = f'/tmp/{zip_name}'
        dataset.export(dataset_folder, format, save_images=True)

        zip_filename = f'/tmp/{dataset_id}.zip'
        zip_file = ZipFile(zip_filename, 'w', ZIP_DEFLATED)
        zip_dir(dataset_folder, zip_file)
        zip_file.close()

        output_key = f's3://{output_bucket}/{output_folder}/{project_id}/{dataset_id}/{zip_name}.zip'
        s3_fs.put(zip_filename, output_key)

        return output_key


def main(event, context):
    for record in event['Records']:
        images_bucket = os.environ['IMAGES_BUCKET']
        output_folder = os.environ['OUTPUT_FOLDER']

        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        project_id, dataset_id, filename = key.split('/')[-3:]
        filename = os.path.splitext(filename)[0]

        print(f's3://{bucket}/{key}')

        with s3_fs.open(f's3://{bucket}/{key}', 'rb') as file:
            data = cloudpickle.load(file)
            dataset = data['dataset']
            format = data['format']

        print(f'Dataset {dataset_id} with filename {filename} from '
              f'project {project_id} contains {len(dataset)} images')

        create_dataset_zip(dataset=dataset,
                           format=format,
                           images_bucket=images_bucket,
                           project_id=project_id,
                           dataset_id=dataset_id,
                           zip_name=filename,
                           output_bucket=bucket,
                           output_folder=output_folder)

        s3_fs.rm(f's3://{bucket}/{key}')


if __name__ == '__main__':
    # This will only run on local development
    keys = s3_fs.glob(f'{os.environ["OUTPUT_BUCKET"]}/**.pkl')
    records = []

    for key in keys:
        records.append({
            'eventName': 'ObjectCreated:PUT',
            "s3": {
                "bucket": {"name": os.environ['OUTPUT_BUCKET'].strip()},
                "object": {"key": '/'.join(key.split('/')[1:])}
            }
        })

    event = {"Records": records}
    print(event)
    main(event, None)
