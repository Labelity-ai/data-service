from typing import List, Optional, Dict, Any, Union, Tuple
from enum import Enum
from functools import partial
from pydantic import BaseModel, validator
from fastapi_utils.camelcase import snake2camel

from app.utils import json_dumps, json_loads
from app.models import EmbeddedModel, ModelConfig, ObjectId, Label, check_relative_points,\
    Tag, Detection, Keypoints, Polygon, Polyline, Caption


class SchemaBase(BaseModel):
    class Config:
        json_loads = json_loads
        json_dumps = json_dumps
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)


class PredictionPostData(SchemaBase):
    label: str
    score: Optional[float] = None
    attributes: Dict[str, Any] = {}


class TagPostData(PredictionPostData):
    value: Optional[Union[str, bool]]


class DetectionPostData(PredictionPostData):
    box: Tuple[float, float, float, float]

    _normalize_box = validator('box', allow_reuse=True)(check_relative_points)


class KeypointsPostData(PredictionPostData):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class PolygonPostData(PredictionPostData):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class PolylinePostData(PredictionPostData):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class CaptionPostData(SchemaBase):
    caption: str
    attributes: Dict[str, Any] = {}


class ImageAnnotationsPostSchema(SchemaBase):
    event_id: str

    points: List[KeypointsPostData] = []
    polylines: List[PolylinePostData] = []
    detections: List[DetectionPostData] = []
    polygons: List[PolygonPostData] = []
    tags: List[TagPostData] = []
    captions: List[CaptionPostData] = []
    attributes: Dict[str, Any] = {}


class ImageAnnotationsData(SchemaBase):
    event_id: str
    thumbnail_url: str = None
    image_url: str = None
    image_width: int = None
    image_height: int = None

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    captions: List[Caption] = []
    attributes: Dict[str, Any] = {}
    labels: List[Label] = []


class ProjectPostSchema(SchemaBase):
    name: str
    description: str


class DatasetPostSchema(SchemaBase):
    name: str
    description: str
    event_ids: List[str]


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


class ApiKey(SchemaBase):
    key: str
    scopes: List[str]


class DatasetToken(SchemaBase):
    token: str
