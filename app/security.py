from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader
from fastapi.security.oauth2 import OAuth2PasswordBearer
from pydantic import BaseModel
from jose import JWTError, jwt
from starlette import status

from app.config import Config
from app.models import User, engine


API_KEY_NAME = 'X-API-Key'
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
OAUTH2_SCHEME = OAuth2PasswordBearer(tokenUrl="token")

ALGORITHM = "HS512"


class TokenData(BaseModel):
    username: Optional[str] = None


def get_project_id(x_api_key: str = Depends(API_KEY_HEADER)):
    """ takes the X-API-Key header and converts it into the matching user object from the database """

    if x_api_key == "1234567890":
        # if passes validation check, return user data for API Key
        # future DB query will go here
        return {
            "id": 1234567890,
            "companies": [1, ],
            "sites": [],
        }

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )


async def get_user(username: str):
    return await engine.find_one(User, User.id == username)


async def get_current_user(token: str = Depends(OAUTH2_SCHEME)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    user = await get_user(username=token_data.username)

    if user is None:
        raise credentials_exception

    return user


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
