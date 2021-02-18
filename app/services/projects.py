from typing import List

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ProjectPostSchema
from app.models import engine, Project, ImageAnnotations, Label


class ProjectService:
    @staticmethod
    async def get_project_by_id(project_id: ObjectId, user_id: ObjectId) -> Project:
        project = await engine.find_one(Project, (Project.id == project_id) & (Project.user_id == user_id))

        if project is None:
            raise HTTPException(404)

        return project

    @staticmethod
    async def get_projects(user_id: ObjectId) -> List[Project]:
        return await engine.find(Project, Project.user_id == user_id)

    @staticmethod
    async def add_project(project: ProjectPostSchema, user_id: ObjectId) -> Project:
        instance = Project(**project.dict(), user_id=user_id)
        return await engine.save(instance)

    @staticmethod
    async def update_project(project_id: ObjectId, project: ProjectPostSchema, user_id: ObjectId) -> Project:
        instance = Project(**project.dict(), id=project_id, user_id=user_id)
        return await engine.save(instance)

    @staticmethod
    async def delete_project(project_id: ObjectId, user_id: ObjectId):
        project = await ProjectService.get_project_by_id(project_id, user_id)
        await engine.delete(project)

    @staticmethod
    async def get_project_labels(project_id: ObjectId) -> List[Label]:
        labels_pipeline = [
            # Select images within specific project
            {'$match': {'project_id': project_id}},
            # Select the labels field and set is as root of the pipeline
            {"$unwind": "$labels"},
            {"$replaceRoot": {"newRoot": "$labels"}},
            # Group labels by shape and name and create the field attributes as list of lists
            {'$group': {
                '_id': {'shape': '$shape', 'name': '$name'},
                'attributes': {
                    '$push': '$attributes'
                }
            }},
            # Return name, shape, and flatten attributes without duplicates.
            {"$project": {
                "name": "$_id.name",
                "shape": "$_id.shape",
                "attributes": {
                    "$reduce": {
                        "input": "$attributes",
                        "initialValue": [],
                        "in": {"$setUnion": ["$$value", "$$this"]}
                    }
                }
            }}
        ]

        collection = engine.get_collection(ImageAnnotations)
        labels = await collection.aggregate(labels_pipeline).to_list(length=None)
        return [Label(name=doc['name'], attributes=doc['attributes'], shape=doc['shape']) for doc in labels]

    @staticmethod
    async def get_project_attributes(project_id: ObjectId) -> List[str]:
        labels_pipeline = [
            {'$match': {'project_id': project_id}},
            {'$unwind': '$attributes'},
            {'$group': {
                '_id': '$_labels.attributes'
            }},
        ]

        collection = engine.get_collection(ImageAnnotations)
        attributes = await collection.aggregate(labels_pipeline).to_list(length=None)
        attributes = [doc['_id'] for doc in attributes]

        return [attr for attr in attributes if attr is not None]
