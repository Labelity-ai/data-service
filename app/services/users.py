from typing import List
from fastapi import HTTPException
from odmantic import ObjectId

from app.models import User, engine
from app.core.tracing import traced


@traced
class UsersService:
    @staticmethod
    async def find_users_by_id(ids: List[ObjectId]) -> List[User]:
        users = await engine.find(User, User.id.in_(ids))
        found_user_ids = set(user.id for user in users)
        missing = set(ids).difference(found_user_ids)

        if missing:
            raise HTTPException(404, f'Users with ids {missing} not found')

        return users
