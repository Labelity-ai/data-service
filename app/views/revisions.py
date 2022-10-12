from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends
from odmantic import ObjectId

from app.schema import RevisionGetSortQuery, SortDirection, RevisionQueryResult, \
    PostRevision, PatchRevision, RevisionChangesQueryResult, PostRevisionChange, PutRevisionChange, \
    PostPutRevisionComment
from app.models import Project, Revision, User, RevisionChange, RevisionComment
from app.security import get_project, get_current_user
from app.services.revisions import RevisionsService
from app.services.users import UsersService
from app.core.tracing import traced

router = InferringRouter(
    tags=["revisions"],
)


@traced
@cbv(router)
class RevisionsView:
    project: Project = Depends(get_project)
    user: User = Depends(get_current_user)

    @router.get("/revision")
    async def get_revision(self,
                           page: int = 0,
                           page_size: int = 10,
                           sort_field: RevisionGetSortQuery = RevisionGetSortQuery.UPDATED_AT,
                           sort_direction: SortDirection = SortDirection.DESCENDING) -> RevisionQueryResult:
        return await RevisionsService.get_revisions(
            project=self.project,
            page=page,
            page_size=page_size,
            sort_field=sort_field,
            sort_direction=sort_direction,
        )

    @router.get("/revision/{id}")
    async def get_revision_by_id(self, id: ObjectId) -> Revision:
        return await RevisionsService.get_revision(id, self.project)

    @router.post("/revision")
    async def create_revision(self, body: PostRevision) -> Revision:
        assignees = await UsersService.find_users_by_id(body.assignees)

        return await RevisionsService.create_revision(
            project=self.project,
            user=self.user,
            assignees=assignees,
            description=body.description
        )

    @router.patch("/revision/{id}")
    async def update_revision(self, id: ObjectId, body: PatchRevision) -> Revision:
        revision = await RevisionsService.get_revision(id, project=self.project)

        if body.assignees is not None:
            assignees = await UsersService.find_users_by_id(body.assignees)
        else:
            assignees = None

        return await RevisionsService.update_revision(
            revision=revision,
            assignees=assignees,
            description=body.description
        )

    @router.put("/revision/{id}")
    async def replace_revision(self, id: ObjectId, body: PostRevision) -> Revision:
        revision = await RevisionsService.get_revision(id, project=self.project)
        assignees = await UsersService.find_users_by_id(body.assignees)

        return await RevisionsService.update_revision(
            revision=revision,
            assignees=assignees,
            description=body.description
        )

    @router.delete("/revision/{id}")
    async def delete_revision(self, id: ObjectId):
        revision = await RevisionsService.get_revision(id, project=self.project)
        return await RevisionsService.delete_revision(revision)

    @router.get("/revision/{id}/changes")
    async def get_revision_changes(self, id: ObjectId, page: int = 0,
                                   page_size: int = 50) -> RevisionChangesQueryResult:
        revision = await RevisionsService.get_revision(id, self.project)
        return await RevisionsService.list_revision_changes(revision, page_size=page_size, page=page)

    @router.post("/revision/{id}/changes")
    async def add_change_to_revision(self, id: ObjectId, data: PostRevisionChange) -> RevisionChange:
        revision = await RevisionsService.get_revision(id, self.project)
        return await RevisionsService.add_change_to_revision(revision, self.user, data)

    @router.put("/revision/changes/{id}")
    async def update_change_to_revision(self, id: ObjectId, data: PutRevisionChange) -> RevisionChange:
        change = await RevisionsService.get_revision_change(id)
        return await RevisionsService.update_revision_change(change, data)

    @router.delete("/revision/changes/{id}")
    async def delete_change_to_revision(self, id: ObjectId) -> RevisionChange:
        change = await RevisionsService.get_revision_change(id)
        return await RevisionsService.delete_revision_change(change)

    @router.post("/revision/changes/{id}/comment")
    async def add_comment_to_revision_change(self,
                                             id: ObjectId,
                                             comment: PostPutRevisionComment) -> RevisionComment:
        change = await RevisionsService.get_revision_change(id)
        return await RevisionsService.add_comment_to_revision_change(change, self.user, comment.content)

    @router.put("/revision/changes/comment/{comment_id}")
    async def edit_revision_change_comment(self,
                                           comment_id: ObjectId,
                                           comment: PostPutRevisionComment) -> RevisionComment:
        return await RevisionsService.edit_revision_comment(comment_id, comment.content)

    @router.delete("/revision/changes/comment/{comment_id}")
    async def delete_revision_change_comment(self, comment_id: ObjectId):
        await RevisionsService.delete_revision_comment(comment_id)
