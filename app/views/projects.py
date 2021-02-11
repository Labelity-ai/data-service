from typing import List

from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends
from odmantic import ObjectId

from app.schema import ProjectPostSchema
from app.models import Project, Label
from app.security import get_project_id
from app.services.projects import ProjectService


router = InferringRouter()


@cbv(router)
class ProjectsView:
    user_id: ObjectId = Depends(get_project_id)  # TODO: Add JWT authentication

    @router.get("/project/{id}")
    async def get_project_by_id(self, id: ObjectId) -> Project:
        return await ProjectService.get_project_by_id(id, self.user_id)

    @router.get("/project")
    async def get_projects(self) -> List[Project]:
        return await ProjectService.get_projects(self.user_id)

    @router.post("/project")
    async def add_project(self, project: ProjectPostSchema) -> Project:
        return await ProjectService.add_project(project, self.user_id)

    @router.put("/project/{id}")
    async def update_project(self, id: ObjectId, project: ProjectPostSchema) -> Project:
        return await ProjectService.update_project(id, project, self.user_id)

    @router.delete("/dataset/{id}")
    async def delete_project(self, id: ObjectId):
        return await ProjectService.delete_project(id, self.user_id)

    @router.get("/project/{id}/labels")
    async def get_dataset_labels(self, id: ObjectId) -> List[Label]:
        # TODO:
        return []
