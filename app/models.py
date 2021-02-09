from typing import Optional, Dict, Any, Tuple, Union, List
from odmantic import Model, ObjectId, EmbeddedModel
from app.utils import json_loads, json_dumps
from pydantic import validator


def check_relative_points(values: List[float]):
    for value in values:
        if value > 1.0:
            raise ValueError('Coordinates should be relative (between 0 and 1)')
    return values


class Prediction(EmbeddedModel):
    score: Optional[float]
    label: str
    attributes: Dict[str, Any]

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class Tag(Prediction):
    score: Optional[float]
    label: str
    value: Optional[Union[str, bool]]
    attributes: Dict[str, Any]


class Detection(Prediction):
    box: Tuple[float, float, float, float]

    @validator('box')
    def check_box(cls, box):
        check_relative_points(box)
        return box


class Keypoint(Prediction):
    point: Tuple[float, float]
    label: str

    @validator('point')
    def check_points(cls, point):
        check_relative_points(point)
        return point


class Polygon(Prediction):
    points: List[Tuple[float, float]]
    label: str

    @validator('points')
    def check_points(cls, points):
        check_relative_points(points)
        return points


class Polyline(Prediction):
    points: List[Tuple[float, float]]
    label: str

    @validator('points')
    def check_points(cls, points):
        check_relative_points(points)
        return points


class ImageAnnotations(Model):
    id: ObjectId
    project_id: ObjectId
    event_id: str

    points: List[Polyline] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygon: List[Polygon] = []
    tags: List[Tag] = []

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps
