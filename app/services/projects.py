from typing import List
import secrets

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ProjectPostSchema, ApiKey
from app.models import engine, Project, ImageAnnotations, Label
from app.core.aggregations import GET_LABELS_PIPELINE, GET_IMAGE_ATTRIBUTES_PIPELINE
from app.core.tracing import traced


@traced
class ProjectService:
    @staticmethod
    async def get_project_by_id(project_id: ObjectId) -> Project:
        project = await engine.find_one(Project, (Project.id == project_id))

        if project is None:
            raise HTTPException(404)

        return project

    @staticmethod
    async def get_projects() -> List[Project]:
        return await engine.find(Project)

    @staticmethod
    async def add_project(project: ProjectPostSchema) -> Project:
        instance = Project(**project.dict())
        return await engine.save(instance)

    @staticmethod
    async def update_project(project_id: ObjectId, project: ProjectPostSchema, user_id: ObjectId) -> Project:
        instance = Project(**project.dict(), id=project_id, user_id=user_id)
        return await engine.save(instance)

    @staticmethod
    async def delete_project(project_id: ObjectId):
        project = await ProjectService.get_project_by_id(project_id)
        await engine.delete(project)

    @staticmethod
    async def add_api_key(project_id: ObjectId) -> ApiKey:
        project = await ProjectService.get_project_by_id(project_id)
        api_key = secrets.token_urlsafe(20)
        # TODO: Check if api key is unique. Is it necessary?
        project.api_keys.extend(api_key)
        await engine.save(project)
        return ApiKey(key=api_key, scopes=['all'])

    @staticmethod
    async def get_project_labels(project_id: ObjectId) -> List[Label]:
        labels_pipeline = [{'$match': {'project_id': project_id}}] + GET_LABELS_PIPELINE
        collection = engine.get_collection(ImageAnnotations)
        labels = await collection.aggregate(labels_pipeline).to_list(length=None)
        return [Label(name=doc['name'], attributes=doc['attributes'], shape=doc['shape']) for doc in labels]

    @staticmethod
    async def get_project_attributes(project_id: ObjectId) -> List[str]:
        labels_pipeline = [{'$match': {'project_id': project_id}}] + GET_IMAGE_ATTRIBUTES_PIPELINE
        collection = engine.get_collection(ImageAnnotations)
        attributes = await collection.aggregate(labels_pipeline).to_list(length=None)
        attributes = [doc['_id'] for doc in attributes]
        return [attr for attr in attributes if attr is not None]
