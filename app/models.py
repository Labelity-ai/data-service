from typing import Optional, Dict, Any, Tuple, Union, List
import enum
from datetime import datetime

from odmantic import Model, ObjectId, EmbeddedModel, AIOEngine, Reference
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


class Keypoint(Prediction):
    point: Tuple[float, float]

    @validator('point')
    def check_points(cls, point):
        check_relative_points(point)
        return point


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

    points: List[Keypoint] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygons: List[Polygon] = []
    tags: List[Tag] = []

    def get_labels(self):
        labels = set()
        # TODO: Infer attributes
        for point in self.points:
            labels.add((point.label, Shape.POINT))

        for polygon in self.polygons:
            labels.add((polygon.label, Shape.POLYGON))

        for polyline in self.polylines:
            labels.add((polyline.label, Shape.POLYLINE))

        for detection in self.detections:
            labels.add((detection.label, Shape.BOX))

        for tag in self.tags:
            labels.add((tag.label, Shape.TAG))

        return [Label(name=name, shape=shape, project_id=self.project_id)
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
    user: User = Reference()


client = AsyncIOMotorClient(Config.MONGO_HOST)
engine = AIOEngine(motor_client=client, database=Config.MONGO_DATABASE)
