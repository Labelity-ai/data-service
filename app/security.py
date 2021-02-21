from typing import Optional

from fastapi import Depends, HTTPException, Header
from fastapi.security.api_key import APIKeyHeader
from fastapi.security.oauth2 import OAuth2PasswordBearer
from jose import JWTError, jwt
from starlette import status

from app.config import Config
from app.models import User, engine, Project, ObjectId

API_KEY_NAME = 'X-API-Key'
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
OAUTH2_SCHEME = OAuth2PasswordBearer(tokenUrl="token")
OAUTH2_SCHEME_OPTIONAL = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

ALGORITHM = "HS512"


async def _get_user(user_id: str):
    return await engine.find_one(User, User.id == user_id)


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


async def _get_current_user(token: str):
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await _get_user(user_id=user_id)

    if user is None:
        raise credentials_exception

    if not user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")

    return user


async def get_current_user(token: str = Depends(OAUTH2_SCHEME)):
    return await _get_current_user(token)


async def get_optional_current_user(token: str = Depends(OAUTH2_SCHEME_OPTIONAL)):
    if not token:
        return None

    return _get_current_user(token)


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


async def get_project(x_api_key: str = Depends(API_KEY_HEADER),
                      user: Optional[User] = Depends(get_optional_current_user),
                      project_id: Optional[str] = Header(None)):
    return await engine.find_one(Project, Project.id == ObjectId('602a2f0deac5ef687b30ac21'))

    if user and project_id:
        project = await engine.find_one(Project, Project.user_id == user.id)
    elif x_api_key:
        project = await engine.find_one(Project, {+Project.api_keys: x_api_key})
    else:
        project = None

    if not project:
        if x_api_key:
            detail = 'Invalid API Key'
        elif user and project_id:
            detail = 'Invalid project_id Header'
        else:
            detail = 'Either API Key or JWT Token + project-id header should be specified'

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail
        )

    return project
