from typing import List

from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import ProjectPostSchema
from app.models import engine, Project


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
