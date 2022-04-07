from typing import List, Optional, Dict, Any, Union, Tuple
from enum import Enum
from datetime import datetime
from functools import partial
from pydantic import BaseModel, validator
from fastapi_utils.camelcase import snake2camel

from app.utils import json_dumps, json_loads
from app.models import ObjectId, Label, check_relative_points,\
    Tag, Detection, Keypoints, Polygon, Polyline, Caption, ImageAnnotations, Node, Edge, RunStatus, \
    Revision, RevisionChange


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


class ImageAnnotationsPutSchema(SchemaBase):
    points: List[KeypointsPostData]
    polylines: List[PolylinePostData]
    detections: List[DetectionPostData]
    polygons: List[PolygonPostData]
    tags: List[TagPostData]
    captions: List[CaptionPostData]
    attributes: Dict[str, Any]


class ImageAnnotationsPostSchema(ImageAnnotationsPutSchema):
    event_id: str


class ImageAnnotationsPatchSchema(SchemaBase):
    points: Optional[List[KeypointsPostData]] = None
    polylines: Optional[List[PolylinePostData]] = None
    detections: Optional[List[DetectionPostData]] = None
    polygons: Optional[List[PolygonPostData]] = None
    tags: Optional[List[TagPostData]] = None
    captions: Optional[List[CaptionPostData]] = None
    attributes: Optional[Dict[str, Any]] = None


class ImageAnnotationsData(SchemaBase):
    event_id: str
    has_image: bool = False
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

    def get_labels(self):
        return ImageAnnotations.get_labels(self)


class ProjectPostSchema(SchemaBase):
    name: str
    description: str


class DatasetPostSchema(SchemaBase):
    name: str
    description: str
    event_ids: List[str]


class DatasetPatchSchema(SchemaBase):
    name: Optional[str] = None
    description: Optional[str] = None
    event_ids: Optional[List[str]] = None


class DatasetGetSortQuery(Enum):
    NAME = 'name'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


class SortDirection(str, Enum):
    ASCENDING = 'ascending'
    DESCENDING = 'descending'


class Pagination(BaseModel):
    page: Optional[int]
    total: Optional[int]


class AnnotationsQueryResult(SchemaBase):
    data: List[ImageAnnotationsData]
    pagination: Pagination
    pipeline_id: Optional[str] = None


class ApiKey(SchemaBase):
    key: str
    scopes: List[str]


class DatasetToken(SchemaBase):
    token: str


class ImageData(SchemaBase):
    event_id: str
    thumbnail_url: str
    original_url: str
    width: int
    height: int

    #created_time: datetime


class PipelinePostData(SchemaBase):
    name: str
    nodes: List[Node]
    edges: List[Edge]
    description: str
    tags: List[str]


class PipelineRunInfo(SchemaBase):
    id: ObjectId
    pipeline_id: ObjectId
    started_at: datetime
    finished_at: datetime
    status: RunStatus
    scheduled_by: ObjectId


class JobId(SchemaBase):
    job_id: str


class PostPutRevisionComment(SchemaBase):
    content: str


class PostRevisionChange(SchemaBase):
    event_id: str
    tags: List[str]


class PutRevisionChange(SchemaBase):
    tags: List[str]


class PostRevision(SchemaBase):
    assignees: List[ObjectId]
    description: str


class PatchRevision(SchemaBase):
    assignees: Optional[List[ObjectId]] = None
    description: Optional[str] = None


class RevisionGetSortQuery(Enum):
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'


class RevisionQueryResult(SchemaBase):
    data: List[Revision]
    pagination: Pagination


class RevisionChangesQueryResult(SchemaBase):
    data: List[RevisionChange]
    pagination: Pagination
