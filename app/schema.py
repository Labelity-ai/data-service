from typing import List
from pydantic import BaseModel
from models import Polyline, Detection, Polygon, Tag
from app.utils import json_dumps, json_loads


class SchemaBase(BaseModel):

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


class ImageAnnotationsPostSchema(SchemaBase):
    event_id: str

    points: List[Polyline] = []
    polylines: List[Polyline] = []
    detections: List[Detection] = []
    polygon: List[Polygon] = []
    tags: List[Tag] = []

    class Config:
        json_loads = json_loads
        json_dumps = json_dumps


