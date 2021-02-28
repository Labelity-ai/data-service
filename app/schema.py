from typing import List, Optional, Dict, Any
from enum import Enum
from functools import partial
from pydantic import BaseModel
from fastapi_utils.camelcase import snake2camel

from app.utils import json_dumps, json_loads
from app.models import Polyline, Detection, Polygon, Tag,\
    EmbeddedModel, ModelConfig, ObjectId, Keypoints, Label


class SchemaBase(BaseModel):
    class Config:
        json_loads = json_loads
        json_dumps = json_dumps
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)


class ImageAnnotationsPostSchema(SchemaBase):
    event_id: str

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    attributes: Dict[str, Any] = {}

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class ImageAnnotationsData(SchemaBase):
    event_id: str
    thumbnail_url: str = None
    image_url: str = None

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    attributes: Dict[str, Any] = {}
    labels: List[Label] = []


class ProjectPostSchema(SchemaBase):
    name: str
    description: str


class DatasetPostSchema(SchemaBase):
    name: str
    description: str


class DatasetGetSortQuery(Enum):
    NAME = 'name'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


class AnnotationsQueryResult(EmbeddedModel):
    class Pagination(BaseModel):
        page: int
        total: int

    data: List[ImageAnnotationsData]
    pagination: Pagination
    pipeline_id: Optional[ObjectId] = None

    Config = ModelConfig


class ApiKey(BaseModel):
    key: str
    scopes: List[str]

