from typing import Optional, Dict, Any, Tuple, Union, List
import enum
from collections import defaultdict
from datetime import datetime

from odmantic import Model, ObjectId, EmbeddedModel, AIOEngine
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import validator

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
    points: List[Tuple[float, float]]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class Polyline(Prediction):
    points: List[Tuple[float, float]]

    _normalize_points = validator('points', allow_reuse=True)(check_relative_points)


class ImageAnnotations(Model):
    event_id: str
    project_id: ObjectId

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    attributes: dict = {}
    _labels: List['Label']

    @staticmethod
    def _extract_labels(objects: List[Prediction], shape: 'Shape', labels: set, attributes):
        for obj in objects:
            key = (obj.label, shape)
            attributes[key].update(obj.attributes.keys())
            labels.add(key)

    def get_labels(self):
        labels = set()
        attributes = defaultdict(set)

        self._extract_labels(self.points, Shape.POINT, labels, attributes)
        self._extract_labels(self.detections, Shape.BOX, labels, attributes)
        self._extract_labels(self.polygons, Shape.POLYGON, labels, attributes)
        self._extract_labels(self.polylines, Shape.POLYLINE, labels, attributes)
        self._extract_labels(self.tags, Shape.TAG, labels, attributes)

        return [Label(name=name,
                      shape=shape,
                      project_id=self.project_id,
                      attributes=list(attributes[(name, shape)]))
                for name, shape in labels]

    Config = ModelConfig


class Shape(enum.Enum):
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


class Dataset(Model):
    name: str
    description: str
    annotations: List[ObjectId]
    project_id: ObjectId

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
    attributes: List[str] = []

    Config = ModelConfig


class QueryExpression(EmbeddedModel):
    field: Optional[str]
    literal: Union[int, float, str, None]
    operator: str
    parameters: Dict[str, Union[float, 'QueryExpression', str]]

    Config = ModelConfig


client = AsyncIOMotorClient(Config.MONGO_HOST)
engine = AIOEngine(motor_client=client, database=Config.MONGO_DATABASE)
