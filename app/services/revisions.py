from typing import List
from fastapi import HTTPException
from datetime import datetime
from odmantic import ObjectId
from pymongo import ASCENDING, DESCENDING

from app.models import ImageAnnotations, engine, RevisionChange, Revision, RevisionComment, Project, User
from app.schema import PostRevision, PatchRevision, PutRevisionChange, SortDirection, RevisionGetSortQuery, \
    RevisionQueryResult, RevisionChangesQueryResult, PostRevisionChange
from app.core.query_engine.stages import make_generic_paginated_pipeline


class RevisionsService:
    @staticmethod
    async def get_revisions(project: Project,
                            page_size: int,
                            page: int,
                            sort_field: RevisionGetSortQuery,
                            sort_direction: SortDirection) -> RevisionQueryResult:
        sort_dir = DESCENDING if sort_direction == SortDirection.DESCENDING else ASCENDING

        pipeline = [
            {'$match': {'project_id': project.id}},
            [{'$sort': {sort_field.value: sort_dir}}]
        ]

        pipeline = make_generic_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)

        pagination = result['metadata'][0] if result['metadata'] else {'page': 0, 'total': 0}
        data = result['data']

        return RevisionQueryResult(data=data, pagination=pagination)

    @staticmethod
    async def get_revision(revision_id: ObjectId, project: Project) -> Revision:
        revision = await engine.find_one(
            Revision,
            (Revision.id == revision_id) & (Revision.project_id == project.id))

        if revision is None:
            raise HTTPException(404)

        return revision

    @staticmethod
    async def create_revision(project: Project, user: User, assignees: List[User], description: str) -> Revision:
        instance = Revision(created_by=user.id,
                            created_at=datetime.now(),
                            project_id=project.id,
                            description=description,
                            assignees=[u.id for u in assignees])
        return await engine.save(instance)

    @staticmethod
    async def update_revision(revision: Revision, assignees: List[User] = None, description: str = None) -> Revision:
        if assignees is not None:
            revision.assignees = assignees
        if description is not None:
            revision.description = description
        revision.updated_at = datetime.now()
        return await engine.save(revision)

    @staticmethod
    async def delete_revision(revision: Revision) -> None:
        await engine.delete(revision)

    @staticmethod
    async def list_revision_changes(revision: Revision,
                                    page_size: int = None,
                                    page: int = None) -> RevisionChangesQueryResult:
        pipeline = [{'$match': {'revision_id': revision.id}}]
        pipeline = make_generic_paginated_pipeline(pipeline, page_size, page)
        collection = engine.get_collection(ImageAnnotations)
        result, *_ = await collection.aggregate(pipeline).to_list(length=None)

        pagination = result['metadata'][0] if result['metadata'] else {'page': 0, 'total': 0}
        data = result['data']

        return RevisionChangesQueryResult(data=data, pagination=pagination)

    @staticmethod
    async def get_revision_change(revision_change_id: ObjectId) -> RevisionChange:
        revision = await engine.find_one(RevisionChange, (RevisionChange.id == revision_change_id))

        if revision is None:
            raise HTTPException(404)

        return revision

    @staticmethod
    async def add_change_to_revision(revision: Revision, user: User, change: PostRevisionChange) -> RevisionChange:
        instance = RevisionChange(
            revision_id=revision.id,
            event_id=change.event_id,
            author_id=user.id,
            tags=change.tags,
            created_date=datetime.now()
        )
        return await engine.save(instance)

    @staticmethod
    async def update_revision_change(change: RevisionChange, new_data: PutRevisionChange) -> RevisionChange:
        change.tags = new_data.tags
        return await engine.save(change)

    @staticmethod
    async def delete_revision_change(change: RevisionChange, delete_comments: bool = True):
        if delete_comments:
            comments = await engine.find(RevisionComment, RevisionComment.revision_change_id == change.id)
            comment_ids = [comment.id for comment in comments]
            await engine.get_collection(RevisionComment).delete_many({'_id': {'$in': comment_ids}})

        await engine.delete(change)

    @staticmethod
    async def add_comment_to_revision_change(change: RevisionChange, user: User, content: str) -> RevisionComment:
        comment = RevisionComment(
            revision_change_id=change.id,
            author_id=user.id,
            content=content,
            created_date=datetime.now(),
            edited_date=None
        )
        return await engine.save(comment)

    @staticmethod
    async def edit_revision_comment(comment_id: ObjectId, content: str) -> RevisionComment:
        comment = await engine.find_one(RevisionComment, RevisionComment.id == comment_id)

        if comment is None:
            raise HTTPException(404)

        comment.content = content
        comment.edited_date = datetime.now()
        return await engine.save(comment)

    @staticmethod
    async def delete_revision_comment(comment_id: ObjectId):
        comment = await engine.find_one(RevisionComment, RevisionComment.id == comment_id)

        if comment is None:
            raise HTTPException(404)

        await engine.delete(comment)
