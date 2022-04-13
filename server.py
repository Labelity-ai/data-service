from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.views.annotations import router as annotations_router
from app.views.projects import router as projects_router
from app.views.datasets import router as datasets_router
from app.views.storage import router as storage_router
from app.views.revisions import router as revisions_router
from app.core.logger import RouteLoggerMiddleware

app = FastAPI(title='Labelity.ai API Service', default_response_class=ORJSONResponse)

origins = [
    'http://localhost',
    'http://localhost:3000',
    'http://localhost:3030',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(annotations_router)
app.include_router(projects_router)
app.include_router(datasets_router)
app.include_router(storage_router)
app.include_router(revisions_router)

prometheus_instrumentator = Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    should_respect_env_var=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=[".*admin.*", "/metrics"],
    env_var_name="ENABLE_METRICS",
)

prometheus_instrumentator.instrument(app).expose(app)

app.add_middleware(RouteLoggerMiddleware)


@app.on_event("startup")
async def startup_event():
    from app.models import initialize
    await initialize()
