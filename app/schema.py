from typing import List
from functools import partial
from enum import Enum
from pydantic import BaseModel
from fastapi_utils.camelcase import snake2camel

from app.models import Polyline, Detection, Polygon, Tag
from app.utils import json_dumps, json_loads


class SchemaBase(BaseModel):
    class Config:
        json_loads = json_loads
        json_dumps = json_dumps
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)


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


class AnnotationsFormat(Enum):
    CVAT_XML = 'CVAT_XML'
    COCO = 'COCO'
    LABELME = 'LABELME_3.0'
