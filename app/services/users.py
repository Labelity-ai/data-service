from typing import List
from fastapi import HTTPException
from odmantic import ObjectId

from app.schema import UserInfo
from app.models import User, engine
from app.core.tracing import traced


@traced
class UsersService:
    @staticmethod
    def create_user_info(user: User) -> UserInfo:
        return UserInfo(name=user.name,
                        email=user.email,
                        verified=bool(user.email_verified),
                        image=user.image)

    @staticmethod
    async def find_users_by_id(ids: List[ObjectId]) -> List[UserInfo]:
        users = await engine.find(User, User.id.in_(ids))
        found_user_ids = set(user.id for user in users)
        missing = set(ids).difference(found_user_ids)

        if missing:
            raise HTTPException(404, f'Users with ids {missing} not found')

        return [UsersService.create_user_info(user) for user in users]

    @staticmethod
    async def get_users() -> List[UserInfo]:
        users = await engine.find(User, User.is_active)
        return [UsersService.create_user_info(user) for user in users]
