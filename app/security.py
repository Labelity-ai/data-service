from fastapi import Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader
from starlette import status

API_KEY_NAME = 'X-API-Key'
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


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
