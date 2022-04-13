from typing import List

from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from fastapi import Depends
from odmantic import ObjectId

from app.schema import UserInfo
from app.models import TestUser
from app.security import get_current_active_user
from app.services.users import UsersService
from app.core.tracing import traced


router = InferringRouter(
    tags=["users"],
)


def testing_user():
    return TestUser(id='602a2960ec631e386e1848a6')


@traced
@cbv(router)
class UsersView:
    # user: User = Depends(get_current_active_user)
    user: TestUser = Depends(testing_user)

    @router.get("/users/me")
    async def ger_user(self) -> UserInfo:
        return UsersService.create_user_info(self.user)

    @router.get("/users")
    async def get_users(self) -> List[UserInfo]:
        return await UsersService.get_users()

