from typing import Optional, Dict, Any, Tuple, Union, List
from odmantic import Model, ObjectId, EmbeddedModel
from app.utils import json_loads, json_dumps


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


class Keypoint(Prediction):
    point: Tuple[float, float]
    label: str


class Polygon(Prediction):
    points: List[Tuple[float, float]]
    label: str


class Polyline(Prediction):
    points: List[Tuple[float, float]]
    label: str


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
