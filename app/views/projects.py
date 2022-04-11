from typing import List

from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi_utils.api_model import APIMessage
from fastapi import Depends
from odmantic import ObjectId

from app.schema import ProjectPostSchema, ApiKey
from app.models import Project, Label, User, TestUser
from app.security import get_current_active_user
from app.services.projects import ProjectService
from app.core.tracing import traced


router = InferringRouter(
    tags=["projects"],
)


def testing_user():
    return TestUser(id='602a2960ec631e386e1848a6')


@traced
@cbv(router)
class ProjectsView:
    # user: User = Depends(get_current_active_user)
    user: TestUser = Depends(testing_user)

    @router.get("/project/{id}")
    async def get_project_by_id(self, id: ObjectId) -> Project:
        return await ProjectService.get_project_by_id(id, self.user.id)

    @router.get("/project")
    async def get_projects(self) -> List[Project]:
        return await ProjectService.get_projects(self.user.id)

    @router.post("/project")
    async def add_project(self, project: ProjectPostSchema) -> Project:
        return await ProjectService.add_project(project, self.user.id)

    @router.post("/project/{id}/api_key")
    async def add_api_key(self, id) -> ApiKey:
        return await ProjectService.add_api_key(id, self.user.id)

    @router.put("/project/{id}")
    async def update_project(self, id: ObjectId, project: ProjectPostSchema) -> Project:
        return await ProjectService.update_project(id, project, self.user.id)

    @router.delete("/project/{id}")
    async def delete_project(self, id: ObjectId) -> APIMessage:
        await ProjectService.delete_project(id, self.user.id)
        return APIMessage(detail=f"Deleted project {id}")

    @router.get("/project/{id}/labels")
    async def get_project_labels(self, id: ObjectId) -> List[Label]:
        return await ProjectService.get_project_labels(id)

    @router.get("/project/{id}/attributes")
    async def get_project_attributes(self, id: ObjectId) -> List[str]:
        return await ProjectService.get_project_attributes(id)
