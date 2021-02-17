from typing import List, Optional, Union, Dict, Any
import random
from datetime import datetime

from pymongo import ASCENDING, DESCENDING

from app.core.query_engine.expressions import ViewExpression, ViewField
from app.core.query_engine.builder import construct_view_expression
from app.models import ObjectId, EmbeddedModel, QueryExpression, ImageAnnotations,\
    Project, Shape, Model, ModelConfig, Label
from pydantic import root_validator


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


class QueryStage(EmbeddedModel):
    stage: str
    parameters: Dict[str, Any]

    @root_validator
    def validate_root(cls, values):
        stage = values['stage']

        if stage not in STAGES:
            raise ValueError(f'Stage {stage} is not a supported type')


class QueryPipeline(Model):
    steps: List[QueryStage]
    created_at: datetime

    Config = ModelConfig


class Exclude(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self):
        expr = ~ViewField('_id').is_in(self.samples)
        return [{'$match': expr.to_mongo()}]

    def validate_stage(self, *_, **__):
        pass


class Exists(EmbeddedModel):
    field: str
    value: bool

    def to_mongo(self):
        if self.value:
            expr = ViewField(self.field).exists().to_mongo()
        else:
            expr = ~ViewField(self.field).exists().to_mongo()

        return [{"$match": expr}]

    def validate_stage(self, *_, **__):
        pass


class Limit(EmbeddedModel):
    limit: int

    def to_mongo(self):
        if self.limit <= 0:
            return [{"$match": {"_id": None}}]

        return [{"$limit": self.limit}]

    def validate_stage(self, *_, **__):
        pass


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

    def validate_stage(self, *_, **__):
        pass


class Match(EmbeddedModel):
    filter: QueryExpression

    def to_mongo(self):
        expr = construct_view_expression(self.filter)
        return [{"$match": expr.to_mongo()}]

    def validate_stage(self, *_, **__):
        pass


class Shuffle(EmbeddedModel):
    seed: Optional[int] = None

    def to_mongo(self, _, **__):
        randint = _get_random_generator(self.seed).randint(int(1e7), int(1e10))

        return [
            {"$set": {"_rand_shuffle": {"$mod": [randint, "$_rand"]}}},
            {"$sort": {"_rand_shuffle": ASCENDING}},
            {"$unset": "_rand_shuffle"},
        ]

    def validate_stage(self, *_, **__):
        pass


class Select(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self, _, **__):
        expr = ViewField('_id').is_in(self.samples)
        return [{"$match": expr.to_mongo()}]

    def validate_stage(self, *_, **__):
        pass


class MatchTags(EmbeddedModel):
    tags: List[str]

    def to_mongo(self):
        expr = ViewField('tags').is_in(self.tags)
        return [{"$match": expr.to_mongo()}]

    def validate_stage(self, *_, **__):
        pass


class MapLabels(EmbeddedModel):
    shape: Shape
    mapping: Dict[str, str]

    def to_mongo(self):
        field = _get_annotations_field(self.shape)

        expression = ViewField(field).map(
            ViewField().set_field("label", ViewField("label").map_values(self.mapping))
        )

        return [{'$set': {field: expression.to_mongo()}}]

    def validate_stage(self, project_labels: List[Label], **_):
        mapping = {(label.name, label.shape) for label in project_labels}
        for label in self.mapping.keys():
            if (label, self.shape) not in mapping:
                raise ValueError(f'Label {label} with shape {self.shape} not found in project task')


class SelectLabels(EmbeddedModel):
    shape: Shape
    labels: List[str]

    def to_mongo(self):
        if not self.labels:
            return []

        filter_labels = FilterLabels(shape=self.shape, filter=ViewField('label').is_in(self.labels))
        return filter_labels.to_mongo()

    def validate_stage(self, project_labels: List[Label], **_):
        mapping = {(label.name, label.shape) for label in project_labels}

        for label in self.labels:
            if (label, self.shape) not in mapping:
                raise ValueError(f'Label {label} with shape {self.shape} not found in project task')


class FilterLabels(EmbeddedModel):
    filter: QueryExpression
    shape: Shape

    def to_mongo(self):
        field = _get_annotations_field(self.shape)
        filter_expr = construct_view_expression(self.filter)
        return [{"$project": {field: ViewField(f'${field}').filter(filter_expr)}}]

    def validate_stage(self, *_, **__):
        pass


class ExcludeLabels(EmbeddedModel):
    shape: Shape
    labels: List[str]

    def to_mongo(self):
        if not self.labels:
            return []

        filter_labels = FilterLabels(shape=self.shape, filter=~ViewField('label').is_in(self.labels))
        return filter_labels.to_mongo()

    def validate_stage(self, project_labels: List[Label], **_):
        mapping = {(label.name, label.shape) for label in project_labels}

        for label in self.labels:
            if (label, self.shape) not in mapping:
                raise ValueError(f'Label {label} with shape {self.shape} not found in project task')


class SelectAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterAttributes(attributes=self.attributes, filter=ViewExpression(True)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterAttributes(attributes=self.attributes, filter=ViewExpression(True))\
            .validate_stage(*args, **kwargs)


class ExcludeAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterAttributes(attributes=self.attributes, filter=ViewExpression(False)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterAttributes(attributes=self.attributes, filter=ViewExpression(False))\
            .validate_stage(*args, **kwargs)


class FilterAttributes(EmbeddedModel):
    attributes: List[str]
    filter: QueryExpression

    def to_mongo(self):
        if not self.attributes:
            return []

        filter_expr = construct_view_expression(self.filter).to_mongo()
        return {"$project": {f'attributes.{attribute}': filter_expr for attribute in self.attributes}}

    def validate_stage(self, project_attributes: List[str], **_):
        for field in self.attributes:
            if field not in project_attributes:
                raise ValueError('Field not found in project attributes')


class SortBy(EmbeddedModel):
    field_or_expression: Union[str, QueryExpression]
    reverse: bool = False

    def to_mongo(self):
        order = DESCENDING if self.reverse else ASCENDING
        field_or_expr = self._get_mongo_field_or_expr()

        if isinstance(field_or_expr, str):
            return [{"$sort": {field_or_expr: order}}]

        return [
            {"$set": {"_sort_field": field_or_expr}},
            {"$sort": {"_sort_field": order}},
            {"$unset": "_sort_field"},
        ]

    def _get_mongo_field_or_expr(self):
        if isinstance(self.field_or_expression, str):
            return self.field_or_expression

        expression = construct_view_expression(self.field_or_expression)

        if isinstance(expression, ViewField):
            return expression.expr
        elif isinstance(expression, ViewExpression):
            return expression.to_mongo()

    def validate_stage(self, project_attributes: List[str], project_labels: List[Label]):
        field_or_expr = self._get_mongo_field_or_expr()

        if isinstance(field_or_expr, str):
            # TODO: Make sure the field exists
            # TODO:  Create an index on the field, if necessary, to make sorting more efficient
            pass


class LimitLabels(EmbeddedModel):
    label: str
    shape: Shape
    limit: int

    def to_mongo(self):
        limit = max(self.limit, 0)
        labels_field = _get_annotations_field(self.shape)

        labels_expr = ViewExpression(f'${labels_field}') \
            .filter(ViewField('label') == self.label) \
            .to_mongo()

        other_labels_expr = ViewExpression(f'${labels_field}') \
            .filter(ViewField('label') != self.label) \
            .to_mongo()

        return [{
            "$set": {
                labels_field: {
                    '$concatArrays': [{"$slice": [labels_expr, limit]}, other_labels_expr]
                }
            }
        }]

    def validate_stage(self, *_, **__):
        pass


class SetAttribute(EmbeddedModel):
    attribute: str
    expression: QueryExpression

    def to_mongo(self):
        expression = construct_view_expression(self.expression)

        if not isinstance(expression, ViewExpression):
            return self.expression

        expression = expression.to_mongo()

        # TODO: Add support for label attributes
        return [{'$set': {f'{+ImageAnnotations.attributes}.{self.attribute}': expression}}]

    def validate_stage(self, project_attributes: List[str], **_):
        # TODO: Add support for nested fields
        if self.attribute not in project_attributes:
            raise ValueError('Field not found in project attributes')


STAGES = {
    'exclude': Exclude,
    'exists': Exists,
    'limit': Limit,
    'skip': Skip,
    'take': Take,
    'match': Match,
    'shuffle': Shuffle,
    'select': Select,
    'match_tags': MatchTags,
    'map_labels': MapLabels,
    'select_labels': SelectLabels,
    'filter_labels': FilterLabels,
    'exclude_labels': ExcludeLabels,
    'select_attributes': SelectAttributes,
    'exclude_attributes': ExcludeAttributes,
    'filter_attributes': FilterAttributes,
    'sort_by': SortBy,
    'limit_labels': LimitLabels,
    'set_attribute': SetAttribute,
}
