from odmantic import AIOEngine
from fastapi import FastAPI

from app.views.annotations import router as annotations_router

app = FastAPI()
engine = AIOEngine()

app.include_router(annotations_router)
