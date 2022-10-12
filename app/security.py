from typing import Optional
from datetime import datetime, timedelta

from fastapi import Depends, HTTPException, Header
from fastapi.security.api_key import APIKeyHeader
from fastapi.security.oauth2 import OAuth2PasswordBearer
from jose import JWTError, jwt
from starlette import status

from app.config import Config
from app.models import User, get_engine, Project, ObjectId, FastToken

API_KEY_NAME = 'X-API-Key'
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
OAUTH2_SCHEME = OAuth2PasswordBearer(tokenUrl="token")
OAUTH2_SCHEME_OPTIONAL = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


async def _get_user(user_id: str):
    engine = await get_engine()
    return await engine.find_one(User, User.id == user_id)


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


async def get_dataset_token(token: str):
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.FAST_TOKEN_JWT_ALGORITHM])
        token_id: str = payload.get("sub")
        if token_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    engine = await get_engine()
    token = await engine.find_one(FastToken, FastToken.id == ObjectId(token_id))

    if token is None:
        raise credentials_exception

    if not token.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Expired token")

    return token


async def _get_current_user(token: str):
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.JWT_ALGORITHM])
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


def create_fast_jwt_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=Config.DATASET_TOKEN_DEFAULT_TIMEDELTA_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.FAST_TOKEN_JWT_ALGORITHM)
    return encoded_jwt


async def get_project(x_api_key: str = Depends(API_KEY_HEADER),
                      user: Optional[User] = Depends(get_optional_current_user),
                      project_id: Optional[str] = Header(None)):
    engine = await get_engine()
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
