from typing import List, Optional, Union
from datetime import datetime, timedelta
from enum import Enum
import json
import tempfile

from fastapi import HTTPException
from odmantic import ObjectId
from zipfile import ZipFile, ZIP_DEFLATED
import cloudpickle

import s3fs
from datumaro.components.dataset import DatasetItem
from datumaro.util.image import Image
from rq.decorators import job
from rq.job import Job

from app.schema import DatasetPostSchema, DatasetGetSortQuery, DatasetToken, \
    ImageAnnotationsData, DatasetPatchSchema
from app.models import ImageAnnotations, Dataset, Label, get_engine, FastToken
from app.services.storage import StorageService
from app.services.annotations import AnnotationsService
from app.core.aggregations import GET_LABELS_PIPELINE
from app.core.exporters import create_datumaro_dataset, DatasetExportFormat
from app.security import create_fast_jwt_token
from app.config import Config
from app.utils import zip_dir
from app.core.queue import redis
from app.core.tracing import traced


s3_fs = s3fs.S3FileSystem()


class DatasetExportingStatus(Enum):
    STARTED = 'started'
    QUEUED = 'queued'
    FINISHED = 'finished'


def _get_dataset_exporting_zip_name(dataset: Dataset, format: DatasetExportFormat):
    return f'{dataset.name}_{format.value}_v{dataset.version}.zip'


def _get_dataset_exporting_result_key(dataset: Dataset, format: DatasetExportFormat):
    return f'{Config.DATASET_ARTIFACTS_BUCKET}/' \
           f'{Config.DATASET_EXPORTING_RESULTS_FOLDER}/' \
           f'{dataset.project_id}/{dataset.name}/{_get_dataset_exporting_zip_name(dataset, format)}'


def _get_dataset_snapshot_key(dataset: Dataset):
    return f'{Config.DATASET_ARTIFACTS_BUCKET}/' \
           f'{Config.DATASET_SNAPSHOT_FOLDER}/' \
           f'{dataset.project_id}/' \
           f'{dataset.id}.json'


@job('dataset', connection=redis)
async def _create_dataset_zip(dataset_binary: bytes, format: DatasetExportFormat):
    dataset: Dataset = cloudpickle.loads(dataset_binary)
    annotations = await DatasetService._get_dataset_snapshot(dataset)
    dataset_datumaro = create_datumaro_dataset(annotations)
    zip_name = _get_dataset_exporting_zip_name(dataset, format)

    with tempfile.TemporaryDirectory() as tmpdir:
        for row in dataset_datumaro:
            item: DatasetItem = row

            if not item.has_image:
                continue

            image_filename = item.image.path.split('/')[-1]
            image_path = f'{tmpdir}/{image_filename}'
            s3_fs.get(f'{Config.IMAGE_STORAGE_BUCKET}/raw/{dataset.project_id}/{image_filename}', image_path)
            item.image = Image(path=image_path, size=item.image.size)

        dataset_folder = f'/tmp/{zip_name}'
        dataset_datumaro.export(dataset_folder, format.value, save_images=True)

        zip_filename = f'/tmp/{dataset.id}.zip'
        zip_file = ZipFile(zip_filename, 'w', ZIP_DEFLATED)
        zip_dir(dataset_folder, zip_file)
        zip_file.close()

        output_key = _get_dataset_exporting_result_key(dataset, format)
        s3_fs.put(zip_filename, output_key)

        return output_key


@traced
class DatasetService:
    @staticmethod
    async def get_dataset_by_id(dataset_id: ObjectId, project_id: ObjectId) -> Dataset:
        engine = await get_engine()
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (Dataset.project_id == project_id))
        if dataset is None:
            raise HTTPException(404)
        return dataset

    @staticmethod
    async def get_annotations_by_dataset_id(dataset_id: ObjectId, project_id: ObjectId) -> List[ImageAnnotationsData]:
        dataset = await DatasetService.get_dataset_by_id(dataset_id, project_id)
        return await DatasetService._get_dataset_snapshot(dataset)

    @staticmethod
    async def delete_dataset(dataset_id: ObjectId, project_id: ObjectId):
        engine = await get_engine()
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (ImageAnnotations.project_id == project_id))

        if dataset is None:
            raise HTTPException(404)

        await engine.delete(dataset)

    @staticmethod
    async def check_event_ids_exist(event_ids: List[str], project_id: ObjectId):
        results = await AnnotationsService.run_raw_annotations_pipeline([
            {'$match': {'event_id': {'$in': event_ids}}}
        ], page_size=None, page=None, project_id=project_id)

        annotations = results.data
        annotations_ids = [x.event_id for x in annotations]

        for event_id in event_ids:
            if event_id not in annotations_ids:
                raise HTTPException(400, f'Annotations for event {event_id} not found')

        return annotations

    @staticmethod
    async def create_dataset(dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        annotations = await DatasetService.check_event_ids_exist(dataset.event_ids, project_id)
        now = datetime.utcnow()
        instance = Dataset(
            **dataset.dict(),
            created_at=now,
            project_id=project_id,
        )
        engine = await get_engine()
        instance = await engine.save(instance)
        await DatasetService._create_dataset_snapshot(instance, annotations)
        return instance

    @staticmethod
    async def update_dataset(dataset: Dataset, new_data: Union[DatasetPatchSchema, DatasetPostSchema]) -> Dataset:
        engine = await get_engine()

        annotations = await DatasetService.check_event_ids_exist(
            dataset.event_ids, dataset.project_id)

        if new_data.event_ids is not None:
            new_data_dict = {k: v for k, v in new_data.dict().items() if v is not None}
            new_data_dict = {
                **dataset.dict(exclude={'id', 'child_id'}),
                **new_data_dict,
                'version': dataset.version + 1,
                'parent_id': dataset.id,
                'created_at': datetime.utcnow(),
                'project_id': dataset.project_id,
            }
            instance = Dataset(**new_data_dict)
            instance = await engine.save(instance)
            dataset.child_id = instance.id
            await engine.save(dataset)
            await DatasetService._create_dataset_snapshot(instance, annotations)
            return instance

        if new_data.name is not None:
            dataset.name = new_data.name

        if new_data.description is not None:
            dataset.description = new_data.description

        return await engine.save(dataset)

    @staticmethod
    async def get_dataset_revisions(dataset: Dataset) -> List[Dataset]:
        pipeline = [
            {'$match': {'_id': dataset.id}},
            {
                '$graphLookup': {
                    'from': 'dataset',
                    'startWith': '$parent_id',
                    'connectFromField': 'parent_id',
                    'connectToField': '_id',
                    'as': 'parents',
                }
            },
            {
                '$graphLookup': {
                    'from': 'dataset',
                    'startWith': '$child_id',
                    'connectFromField': 'child_id',
                    'connectToField': '_id',
                    'as': 'children',
                }
            }
        ]

        engine = await get_engine()
        collection = engine.get_collection(Dataset)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)

        return [Dataset.parse_doc(x) for x in result['parents']] + \
               [dataset] + \
               [Dataset.parse_doc(x) for x in result['children']]

    @staticmethod
    async def get_datasets(project_id: ObjectId,
                           name: Optional[str] = None,
                           include_all_revisions: bool = False,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        queries = [Dataset.project_id == project_id]
        if name:
            queries.append(Dataset.name == name)

        if not include_all_revisions:
            queries.append(Dataset.child_id == None)

        kwargs = {'sort': getattr(Dataset, sort.value)} if sort else {}
        engine = await get_engine()
        return await engine.find(Dataset, *queries, **kwargs)

    @staticmethod
    async def get_dataset_labels(dataset: Dataset) -> List[Label]:
        labels_pipeline = [{
            '$match': {
                'project_id': dataset.project_id,
                'event_id': {'$in': dataset.event_ids}
            }
        }] + GET_LABELS_PIPELINE
        engine = await get_engine()
        collection = engine.get_collection(ImageAnnotations)
        labels = await collection.aggregate(labels_pipeline).to_list(length=None)
        return [Label(name=doc['name'], attributes=doc['attributes'], shape=doc['shape']) for doc in labels]

    @staticmethod
    async def get_dataset_download_url(job_id: str) -> Optional[str]:
        try:
            job = Job.fetch(job_id, connection=redis)
            key = job.result
            if not key:
                return None
            return await StorageService.create_presigned_get_url_for_object_download(key)
        except Exception:
            return None

    @staticmethod
    async def download_dataset(dataset: Dataset, format: DatasetExportFormat) -> DatasetExportingStatus:
        dataset_binary = cloudpickle.dumps(dataset)
        job = _create_dataset_zip.delay(dataset_binary=dataset_binary, format=format)
        return job.id

    @staticmethod
    async def create_access_token(dataset: Dataset, expires_delta: Optional[timedelta] = None):
        engine = await get_engine()
        token = await engine.save(FastToken(
            creation_date=datetime.utcnow(),
            timestamp=datetime.utcnow(),
            dataset_id=dataset.id,
            project_id=dataset.project_id
        ))
        token = create_fast_jwt_token({'sub': str(token.id)}, expires_delta)
        return DatasetToken(token=token)

    @staticmethod
    async def _create_dataset_snapshot(dataset: Dataset, annotations: List[ImageAnnotationsData]):
        key = _get_dataset_snapshot_key(dataset)
        with s3_fs.open(key, 'w') as file:
            data = [x.dict() for x in annotations]
            json.dump(data, file)

    @staticmethod
    async def _get_dataset_snapshot(dataset: Dataset):
        key = _get_dataset_snapshot_key(dataset)
        with s3_fs.open(key, 'r') as file:
            data = json.load(file)
            return [ImageAnnotationsData.parse_obj(x) for x in data]
