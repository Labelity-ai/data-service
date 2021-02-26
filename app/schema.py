from typing import List, Optional, Dict, Any
from enum import Enum
from functools import partial
from pydantic import BaseModel
from fastapi_utils.camelcase import snake2camel

from app.utils import json_dumps, json_loads
from app.models import Polyline, Detection, Polygon, Tag, ImageAnnotations,\
    EmbeddedModel, ModelConfig, ObjectId


class SchemaBase(BaseModel):
    class Config:
        json_loads = json_loads
        json_dumps = json_dumps
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)


class ImageAnnotationsPostSchema(SchemaBase):
    event_id: str

    points: List[Polyline] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    attributes: Dict[str, Any] = {}

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class ProjectPostSchema(SchemaBase):
    name: str
    description: str


class DatasetPostSchema(SchemaBase):
    name: str
    description: str


class DatasetGetSortQuery(Enum):
    NAME = 'name'


class AnnotationsQueryResult(EmbeddedModel):
    class Pagination(BaseModel):
        page: int
        total: int

    data: List[ImageAnnotations]
    pagination: Pagination
    pipeline_id: Optional[ObjectId] = None

    Config = ModelConfig


class ApiKey(BaseModel):
    key: str
    scopes: List[str]

