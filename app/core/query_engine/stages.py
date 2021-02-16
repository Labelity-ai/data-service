from typing import List, Optional, Union, Dict
from abc import ABCMeta
import random

from pymongo import ASCENDING, DESCENDING

from app.core.query_engine.expressions import ViewExpression, ViewField
from app.core.query_engine.builder import construct_view_expression
from app.models import ObjectId, EmbeddedModel, QueryExpression, ImageAnnotations, Project, Shape


def _get_random_generator(seed):
    if seed is None:
        return random

    _random = random.Random()
    _random.seed(seed)
    return _random


def _get_annotations_field(shape: Shape):
    if shape == Shape.TAG:
        return 'tag'
    elif shape == Shape.BOX:
        return 'detections'
    elif shape == Shape.POINT:
        return 'points'
    elif shape == Shape.POLYGON:
        return 'polygons'
    elif shape == Shape.POLYLINE:
        return 'polylines'


class Exclude(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self):
        expr = ~ViewField('_id').is_in(self.samples)
        return [{'$match': expr.to_mongo()}]


class Exists(EmbeddedModel):
    field: str
    value: bool

    def to_mongo(self):
        if self.value:
            expr = ViewField(self.field).exists().to_mongo()
        else:
            expr = ~ViewField(self.field).exists().to_mongo()

        return [{"$match": expr}]


class Limit(EmbeddedModel):
    limit: int

    def to_mongo(self):
        if self.limit <= 0:
            return [{"$match": {"_id": None}}]

        return [{"$limit": self.limit}]


class Skip(EmbeddedModel):
    skip: int

    def to_mongo(self, _, **__):
        if self.skip <= 0:
            return []

        return [{"$skip": self.skip}]


class Take(EmbeddedModel):
    size: int
    seed: Optional[int] = None

    def to_mongo(self, _, **__):
        randint = _get_random_generator(self.seed).randint(int(1e7), int(1e10))

        if self.size <= 0:
            return [{"$match": {"_id": None}}]

        return [
            {"$set": {"_rand_take": {"$mod": [randint, "$_rand"]}}},
            {"$sort": {"_rand_take": ASCENDING}},
            {"$limit": self.size},
            {"$unset": "_rand_take"},
        ]


class Match(EmbeddedModel):
    filter: QueryExpression

    def to_mongo(self):
        expr = construct_view_expression(self.filter)
        return [{"$match": expr.to_mongo()}]


class Shuffle(EmbeddedModel):
    seed: Optional[int] = None

    def to_mongo(self, _, **__):
        randint = _get_random_generator(self.seed).randint(int(1e7), int(1e10))

        return [
            {"$set": {"_rand_shuffle": {"$mod": [randint, "$_rand"]}}},
            {"$sort": {"_rand_shuffle": ASCENDING}},
            {"$unset": "_rand_shuffle"},
        ]


class Select(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self, _, **__):
        expr = ViewField('_id').is_in(self.samples)
        return [{"$match": expr.to_mongo()}]


class MatchTags(EmbeddedModel):
    tags: List[str]

    def to_mongo(self):
        expr = ViewField('tags').is_in(self.tags)
        return [{"$match": expr.to_mongo()}]


class MapLabels(EmbeddedModel):
    shape: Shape
    mapping: Dict[str, str]

    def to_mongo(self):
        field = _get_annotations_field(self.shape)

        expression = ViewField(field).map(
            ViewField().set_field("label", ViewField("label").map_values(self.mapping))
        )

        return [{'$set': {field: expression.to_mongo()}}]

    def validate(self, project: Project):
        mapping = {(label.name, label.shape) for label in project.labels}
        for label in self.mapping.keys():
            if (label, self.shape) not in mapping:
                raise ValueError(f'Label {label} with shape {self.shape} not found in project task')


STAGES = {
    'exists': Exists,
    'limit': Limit,
    'skip': Skip,
    'take': Take,
    'match': Match,
    'shuffle': Shuffle,
    'select': Select,
}
