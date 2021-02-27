from typing import List, Optional
from datetime import datetime

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ImageAnnotationsPostSchema, DatasetPostSchema, DatasetGetSortQuery
from app.models import ImageAnnotations, Dataset, Label, engine
from app.services.annotations import AnnotationsService
from app.core.aggregations import GET_LABELS_PIPELINE


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
    async def get_annotations_by_dataset_id(dataset_id: ObjectId, project_id: ObjectId) -> List[ImageAnnotations]:
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (Dataset.project_id == project_id))

        if dataset is None:
            raise HTTPException(404)

        return await engine.find(ImageAnnotations, ImageAnnotations.id.in_(dataset.annotations))

    @staticmethod
    async def delete_dataset(dataset_id: ObjectId, project_id: ObjectId):
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (ImageAnnotations.project_id == project_id))

        if dataset is None:
            raise HTTPException(404)

        await engine.delete(dataset)

    @staticmethod
    async def attach_annotations_to_dataset(dataset_id: ObjectId,
                                            annotations_ids: List[ObjectId],
                                            annotations: List[ImageAnnotationsPostSchema],
                                            project_id: ObjectId) -> List[ImageAnnotations]:
        dataset = await engine.find_one(
            Dataset,
            (Dataset.id == dataset_id) & (Dataset.project_id == project_id))

        if dataset is None:
            raise HTTPException(404)

        annotations_instances = await AnnotationsService.add_annotations_bulk(annotations, project_id)
        dataset.annotations.extend(annotations_ids)
        dataset.annotations.extend([annotations.id for annotations in annotations_instances])
        engine.save(dataset)

        return await DatasetService.get_annotations_by_dataset_id(dataset_id, project_id)

    @staticmethod
    async def create_dataset(dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        now = datetime.utcnow()
        instance = Dataset(**dataset.dict(), created_at=now, updated_at=now, project_id=project_id)
        return await engine.save(instance)

    @staticmethod
    async def update_dataset(dataset_id: ObjectId, dataset: DatasetPostSchema, project_id: ObjectId) -> Dataset:
        instance = Dataset(
            **dataset.dict(), id=dataset_id, updated_at=datetime.utcnow(), project_id=project_id)
        return await engine.save(instance)

    @staticmethod
    async def get_datasets(project_id: ObjectId,
                           name: Optional[str] = None,
                           sort: DatasetGetSortQuery = None) -> List[Dataset]:
        queries = [Dataset.project_id == project_id]
        if name:
            queries.append(Dataset.name == name)
        return await engine.find(Dataset, *queries, sort=sort.value)

    @staticmethod
    async def get_dataset_labels(dataset_id: ObjectId, project_id) -> List[Label]:
        labels_pipeline = [{'$match': {'project_id': project_id, 'dataset_id': dataset_id}}] + GET_LABELS_PIPELINE
        collection = engine.get_collection(ImageAnnotations)
        labels = await collection.aggregate(labels_pipeline).to_list(length=None)
        return [Label(name=doc['name'], attributes=doc['attributes'], shape=doc['shape']) for doc in labels]
