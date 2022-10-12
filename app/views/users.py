from typing import List

from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends

from app.schema import UserInfo
from app.models import User
from app.security import get_current_user
from app.services.users import UsersService
from app.core.tracing import traced


router = InferringRouter(tags=["users"])


@traced
@cbv(router)
class UsersView:
    user: User = Depends(get_current_user)

    @router.get("/users/me")
    async def get_user(self) -> UserInfo:
        return UsersService.get_user_info(self.user)

    @router.get("/users")
    async def get_users(self) -> List[UserInfo]:
        return await UsersService.get_users()

