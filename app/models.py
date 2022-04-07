from __future__ import annotations
from typing import Optional, Dict, Any, Tuple, Union, List
import enum
from collections import defaultdict
from datetime import datetime

from odmantic import Model, ObjectId, EmbeddedModel, AIOEngine
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING
from pydantic import validator, BaseModel

from app.utils import json_loads, json_dumps
from app.config import Config


def check_relative_points(values: List[float]):
    for value in values:
        if value > 1.0:
            raise ValueError('Coordinates should be relative (between 0 and 1)')
    return values


class ModelConfig:
    json_loads = json_loads
    json_dumps = json_dumps


class Prediction(EmbeddedModel):
    label: str
    group: str = 'ground_truth'
    score: Optional[float] = None
    attributes: Dict[str, Any] = {}

    Config = ModelConfig


class Tag(Prediction):
    value: Optional[Union[str, bool]]


class Detection(Prediction):
    box: Tuple[float, float, float, float]

    _normalize_box = validator('box', allow_reuse=True)(check_relative_points)


class Keypoints(Prediction):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class Polygon(Prediction):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class Polyline(Prediction):
    points: List[float]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class Caption(EmbeddedModel):
    caption: str
    group: str = 'ground_truth'
    attributes: Dict[str, Any] = {}

    Config = ModelConfig


class Shape(str, enum.Enum):
    BOX = 'box'
    TAG = 'tag'
    POINT = 'point'
    POLYGON = 'polygon'
    POLYLINE = 'polyline'


class Label(EmbeddedModel):
    name: str
    shape: Shape
    attributes: List[str]

    Config = ModelConfig


class ImageAnnotations(Model):
    event_id: str
    project_id: ObjectId
    has_image: bool = False

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    captions: List[Caption] = []
    attributes: Dict[str, Any] = {}
    labels: List[Label] = []

    @staticmethod
    def _extract_labels(objects: List[Prediction], shape: Shape, labels: set, attributes):
        for obj in objects:
            key = (obj.label, shape, obj.group)
            attributes[key].update(obj.attributes.keys())
            labels.add(key)

    def get_labels(self):
        labels = set()
        attributes = defaultdict(set)

        ImageAnnotations._extract_labels(self.points, Shape.POINT, labels, attributes)
        ImageAnnotations._extract_labels(self.detections, Shape.BOX, labels, attributes)
        ImageAnnotations._extract_labels(self.polygons, Shape.POLYGON, labels, attributes)
        ImageAnnotations._extract_labels(self.polylines, Shape.POLYLINE, labels, attributes)
        ImageAnnotations._extract_labels(self.tags, Shape.TAG, labels, attributes)

        return [Label(name=name,
                      shape=shape,
                      attributes=list(attributes[(name, shape, group)]))
                for name, shape, group in labels]

    Config = ModelConfig


class Dataset(Model):
    name: str
    version: int = 1
    description: str
    event_ids: List[str] = []
    project_id: ObjectId
    parent_id: Optional[ObjectId] = None
    child_id: Optional[ObjectId] = None
    created_at: datetime

    Config = ModelConfig


class User(Model):
    name: str
    email: str
    email_verified: datetime
    is_active: bool
    image: str
    created_at: datetime
    updated_at: datetime

    Config = ModelConfig


class TestUser(Model):
    pass


class Project(Model):
    name: str
    description: str
    user_id: ObjectId
    api_keys: List[str] = []

    Config = ModelConfig


class Image(Model):
    project_id: ObjectId
    event_id: str
    width: int
    height: int

    #created_time: datetime

    Config = ModelConfig


class FastToken(Model):
    is_active: bool = True
    creation_date: datetime
    timestamp: datetime
    dataset_id: ObjectId
    project_id: ObjectId

    Config = ModelConfig


class QueryExpression(BaseModel):
    field: Optional[str]
    literal: Union[int, float, str, None]
    operator: Optional[str]
    parameters: Dict[str, Union[float, 'QueryExpression', str]] = {}
    # created_at: datetime = Field(default_factory=datetime.utcnow)

    Config = ModelConfig


class NodeOperation(enum.Enum):
    ANNOTATIONS = 'annotations'
    WEBHOOK = 'webhook'
    DATASET = 'dataset'
    CVAT = 'cvat'


class NodeType(enum.Enum):
    INPUT = 'input'
    PROCESSING = 'processing'
    OUTPUT = 'output'


class RunStatus(enum.Enum):
    SUCCESS = 'success'
    IN_PROGRESS = 'in_progress'
    FAILED = 'failed'


class Node(EmbeddedModel):
    type: NodeType
    operation: NodeOperation
    parameters: dict


class Edge(EmbeddedModel):
    input_node: int
    output_node: int


class Pipeline(Model):
    name: str
    project_id: ObjectId
    nodes: List[Node]
    edges: List[Edge]
    description: str = ''
    tags: List[str] = []
    deleted: bool = False


class PipelineRun(Model):
    pipeline_id: ObjectId
    job_id: str
    scheduled_by: Optional[ObjectId]
    started_at: datetime
    finished_at: Optional[datetime]
    status: RunStatus


class RevisionComment(EmbeddedModel):
    author_id: ObjectId
    content: str
    created_date: datetime
    edited_date: datetime


class RevisionChange(Model):
    event_id: str
    revision_id: ObjectId
    author_id: ObjectId
    added_tags: List[str]
    removed_tags: List[str]
    created_date: datetime
    edited_date: datetime
    comments: List[RevisionComment]


class Revision(Model):
    project_id: ObjectId
    created_by: ObjectId
    created_at: datetime
    assignee: List[ObjectId]
    changes: List[RevisionChange]
    description: str


QueryExpression.update_forward_refs()

client = AsyncIOMotorClient(Config.MONGO_HOST)
engine = AIOEngine(motor_client=client, database=Config.MONGO_DATABASE)


async def initialize():
    await engine.get_collection(Project).create_index('user_id')
    await engine.get_collection(Dataset).create_index([
        ('project_id', DESCENDING),
        ('name', DESCENDING),
        ('version', DESCENDING)
    ])
    await engine.get_collection(ImageAnnotations).create_index([
        ('project_id', DESCENDING),
        ('has_image', DESCENDING),
        ('event_id', DESCENDING),
    ])
    await engine.get_collection(ImageAnnotations).create_index('attributes.$**')
    await engine.get_collection(ImageAnnotations).create_index('labels.$**')
    await engine.get_collection(Image).create_index([
        ('project_id', DESCENDING),
        ('event_id', DESCENDING)
    ], unique=True)
    await engine.get_collection(Image).create_index('created_time')
    await engine.get_collection(FastToken).create_index('dataset_id')
    await engine.get_collection(PipelineRun).create_index('pipeline_id')

    await engine.get_collection(Revision).create_index([
        ('project_id', DESCENDING),
        ('created_by', DESCENDING),
    ])

    await engine.get_collection(RevisionChange).create_index([
        ('revision_id', DESCENDING),
        ('event_id', DESCENDING),
        ('author_id', DESCENDING),
    ])
