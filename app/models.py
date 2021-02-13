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


class Prediction(EmbeddedModel):
    label: str
    score: Optional[float] = None
    attributes: Dict[str, Any] = {}

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class Tag(Prediction):
    value: Optional[Union[str, bool]]


class Detection(Prediction):
    box: Tuple[float, float, float, float]

    @validator('box')
    def check_box(cls, box):
        check_relative_points(box)
        return box


class Keypoints(Prediction):
    points: List[float, float]

    @validator('points')
    def check_points(cls, points):
        check_relative_points(points)
        return points


class Polygon(Prediction):
    points: List[Tuple[float, float]]

    @validator('points')
    def check_points(cls, points):
        check_relative_points(points)
        return points


class Polyline(Prediction):
    points: List[Tuple[float, float]]

    @validator('points')
    def check_points(cls, points):
        check_relative_points(points)
        return points


class ModelBase(Model):
    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class ImageAnnotations(ModelBase):
    event_id: str
    project_id: ObjectId

    points: List[Keypoints] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []
    attributes: dict = {}

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
                      attributes=attributes[(name, shape)])
                for name, shape in labels]


class Shape(enum.Enum):
    BOX = 'box'
    TAG = 'tag'
    POINT = 'point'
    POLYGON = 'polygon'
    POLYLINE = 'polyline'


class AttributeType(enum.Enum):
    TEXT = 'TEXT'
    SELECT = 'SELECT'
    RADIO = 'RADIO'
    CHECKBOX = 'CHECKBOX'
    NUMBER = 'NUMBER'


class Attribute(EmbeddedModel):
    name: str
    type: AttributeType
    value: List[str]

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class Label(ModelBase):
    name: str
    shape: Shape
    attributes: List[Attribute]
    project_id: ObjectId


class Dataset(ModelBase):
    name: str
    description: str
    annotations: List[ObjectId]
    project_id: ObjectId


class User(ModelBase):
    name: str
    email: str
    email_verified: datetime
    is_active: bool
    image: str
    created_at: datetime
    updated_at: datetime


class Project(ModelBase):
    name: str
    description: str
    user_id: ObjectId


client = AsyncIOMotorClient(Config.MONGO_HOST)
engine = AIOEngine(motor_client=client, database=Config.MONGO_DATABASE)
