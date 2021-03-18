from typing import List, Optional
from datetime import datetime, timedelta
from enum import Enum

import cloudpickle
from fastapi import HTTPException
from odmantic import ObjectId
import s3fs

from app.schema import DatasetPostSchema, DatasetGetSortQuery, DatasetToken, ImageAnnotationsData
from app.models import ImageAnnotations, Dataset, Label, engine, FastToken
from app.services.storage import StorageService
from app.services.annotations import AnnotationsService
from app.core.aggregations import GET_LABELS_PIPELINE
from app.core.exporters import create_datumaro_dataset, DatasetExportFormat
from app.security import create_fast_jwt_token
from app.config import Config

s3_fs = s3fs.S3FileSystem()


class DatasetExportingStatus(Enum):
    STARTED = 'started'
    QUEUED = 'queued'
    FINISHED = 'finished'


def _get_dataset_exporting_result_key(dataset: Dataset, format: DatasetExportFormat):
    return f'{Config.DATASET_ARTIFACTS_BUCKET}/' \
           f'{Config.DATASET_EXPORTING_RESULTS_FOLDER}/' \
           f'{dataset.project_id}/{dataset.name}/{dataset.name}_{format.value}_v{dataset.version}.zip'


def _get_dataset_exporting_request_key(dataset: Dataset, format: DatasetExportFormat):
    return f'{Config.DATASET_ARTIFACTS_BUCKET}/' \
           f'{Config.DATASET_EXPORTING_QUEUE_FOLDER}/' \
           f'{dataset.project_id}/{dataset.name}/{dataset.name}_{format.value}_v{dataset.version}.pkl'


class DatasetService:
    @staticmethod
    async def get_dataset_by_id(dataset_id: ObjectId, project_id: ObjectId) -> Dataset:
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (Dataset.project_id == project_id))
        if dataset is None:
            raise HTTPException(404)
        return dataset

    @staticmethod
    async def get_annotations_by_dataset_id(dataset_id: ObjectId, project_id: ObjectId) -> List[ImageAnnotationsData]:
        dataset = await DatasetService.get_dataset_by_id(dataset_id, project_id)

        result = await AnnotationsService.run_raw_annotations_pipeline(
            [{'$match': {'event_id': {'$in': dataset.event_ids}}}],
            page=0, page_size=int(10e10), project_id=project_id)

        return result.data

    @staticmethod
    async def delete_dataset(dataset_id: ObjectId, project_id: ObjectId):
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (ImageAnnotations.project_id == project_id))

        if dataset is None:
            raise HTTPException(404)

        await engine.delete(dataset)

    @staticmethod
    async def check_event_ids_exist(event_ids: List[str], project_id: ObjectId):
        event_ids_pipeline = [
            {'$match': {'project_id': project_id, 'event_id': {'$in': event_ids}}},
            {'$project': {'_id': 1, 'event_id': 1}},
        ]

        collection = engine.get_collection(ImageAnnotations)
        annotations_ids = await collection.aggregate(event_ids_pipeline).to_list(length=None)
        annotations_ids = {doc['event_id']: doc['_id'] for doc in annotations_ids}

        for event_id in event_ids:
            if event_id not in annotations_ids:
                raise HTTPException(400, f'Annotations for event {event_id} not found')

    @staticmethod
    async def create_dataset(dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        await DatasetService.check_event_ids_exist(dataset.event_ids, project_id)
        now = datetime.utcnow()
        instance = Dataset(
            **dataset.dict(),
            created_at=now,
            updated_at=now,
            project_id=project_id,
        )
        return await engine.save(instance)

    @staticmethod
    async def update_dataset(dataset_id: ObjectId, dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        await DatasetService.check_event_ids_exist(dataset.event_ids, project_id)
        instance = Dataset(
            **dataset.dict(),
            id=dataset_id,
            updated_at=datetime.utcnow(),
            project_id=project_id,
        )
        return await engine.save(instance)

    @staticmethod
    async def get_datasets(project_id: ObjectId,
                           name: Optional[str] = None,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        queries = [Dataset.project_id == project_id]
        if name:
            queries.append(Dataset.name == name)
        kwargs = {'sort': getattr(Dataset, sort.value)} if sort else {}
        return await engine.find(Dataset, *queries, **kwargs)

    @staticmethod
    async def get_dataset_labels(dataset: Dataset) -> List[Label]:
        labels_pipeline = [{
            '$match': {
                'project_id': dataset.project_id,
                'event_id': {'$in': dataset.event_ids}
            }
        }] + GET_LABELS_PIPELINE
        collection = engine.get_collection(ImageAnnotations)
        labels = await collection.aggregate(labels_pipeline).to_list(length=None)
        return [Label(name=doc['name'], attributes=doc['attributes'], shape=doc['shape']) for doc in labels]

    @staticmethod
    async def get_dataset_download_url(dataset: Dataset, format: DatasetExportFormat) -> str:
        key = _get_dataset_exporting_result_key(dataset, format)
        return await StorageService.create_presigned_get_url_for_object_download(key)

    @staticmethod
    async def download_dataset(dataset: Dataset, format: DatasetExportFormat) -> DatasetExportingStatus:
        request_file_key = _get_dataset_exporting_request_key(dataset, format)
        result_file_key = _get_dataset_exporting_result_key(dataset, format)

        s3_fs.invalidate_cache()

        if s3_fs.exists(result_file_key):
            try:
                s3_fs.rm(request_file_key)
            except:
                pass
            finally:
                return DatasetExportingStatus.FINISHED

        if s3_fs.exists(request_file_key):
            return DatasetExportingStatus.QUEUED

        annotations = await DatasetService.get_annotations_by_dataset_id(dataset.id, dataset.project_id)
        dataset = create_datumaro_dataset(annotations)

        with s3_fs.open(request_file_key, 'wb') as file:
            data = {'dataset': dataset, 'format': format.value}
            cloudpickle.dump(data, file)

        return DatasetExportingStatus.STARTED

    @staticmethod
    async def create_access_token(dataset: Dataset, expires_delta: Optional[timedelta] = None):
        token = await engine.save(FastToken(
            creation_date=datetime.utcnow(),
            timestamp=datetime.utcnow(),
            dataset_id=dataset.id,
            project_id=dataset.project_id
        ))
        token = create_fast_jwt_token({'sub': str(token.id)}, expires_delta)
        return DatasetToken(token=token)
