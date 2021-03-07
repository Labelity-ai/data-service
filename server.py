from odmantic import AIOEngine
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from app.views.annotations import router as annotations_router
from app.views.projects import router as projects_router
from app.views.datasets import router as datasets_router
from app.views.storage import router as storage_router

app = FastAPI(title='Labelity.ai API Service', default_response_class=ORJSONResponse)
engine = AIOEngine()

app.include_router(annotations_router)
app.include_router(projects_router)
app.include_router(datasets_router)
app.include_router(storage_router)


@app.on_event("startup")
async def startup_event():
    from app.models import initialize
    await initialize()
