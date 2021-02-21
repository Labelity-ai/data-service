from typing import List, Optional, Union, Dict, Any
import random
from datetime import datetime
from enum import Enum

from pymongo import ASCENDING, DESCENDING

from app.core.query_engine.expressions import ViewExpression, ViewField
from app.core.query_engine.builder import construct_view_expression
from app.models import ObjectId, EmbeddedModel, QueryExpression, ImageAnnotations,\
    Shape, Model, ModelConfig, Label, Field
from pydantic import root_validator, create_model


def _get_random_generator(seed):
    if seed is None:
        return random

    _random = random.Random()
    _random.seed(seed)
    return _random


def _get_annotations_field(shape: Union[Shape, str]):
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


def _get_projection_stage(project_expr):
    return {'$project': {
        +ImageAnnotations.project_id: 1,
        +ImageAnnotations.event_id: 1,
        +ImageAnnotations.attributes: 1,
        **project_expr
    }}


class QueryStage(EmbeddedModel):
    stage: str
    parameters: Dict[str, Any]

    @root_validator
    def validate_root(cls, values):
        stage = values['stage']

        if stage not in STAGES:
            raise ValueError(f'Stage {stage} is not a supported type')

        return values


class QueryPipeline(Model):
    steps: List[QueryStage]
    project_id: ObjectId
    created_at: datetime = Field(default_factory=datetime.utcnow)
    dataset_id: Optional[ObjectId] = None

    Config = ModelConfig


class Exclude(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self):
        return [{'$match': {'_id': {'$not': {'$in': self.samples}}}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Exists(EmbeddedModel):
    field: str
    value: bool = True

    def to_mongo(self):
        expr = ViewField(self.field).exists(self.value)
        return [{"$match": {'$expr': expr.to_mongo()}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Limit(EmbeddedModel):
    limit: int

    def to_mongo(self):
        if self.limit <= 0:
            return [{"$match": {"_id": None}}]

        return [{"$limit": self.limit}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Skip(EmbeddedModel):
    skip: int

    def to_mongo(self):
        if self.skip <= 0:
            return []

        return [{"$skip": self.skip}]

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Take(EmbeddedModel):
    size: int
    seed: Optional[int] = None

    def to_mongo(self):
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

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Match(EmbeddedModel):
    filter: QueryExpression

    def to_mongo(self):
        expr = construct_view_expression(self.filter)
        return [{"$match": {'$expr': expr.to_mongo()}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Shuffle(EmbeddedModel):
    seed: Optional[int] = None

    def to_mongo(self):
        randint = _get_random_generator(self.seed).randint(int(1e7), int(1e10))

        return [
            {"$set": {"_rand_shuffle": {"$mod": [randint, "$_rand"]}}},
            {"$sort": {"_rand_shuffle": ASCENDING}},
            {"$unset": "_rand_shuffle"},
        ]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Select(EmbeddedModel):
    samples: List[ObjectId]

    def to_mongo(self):
        return [{'$match': {'_id': {'$in': self.samples}}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class MatchTags(EmbeddedModel):
    tags: List[str]

    def to_mongo(self):
        expr = ViewField('tags').is_in(self.tags)
        return [{"$match": {'$expr': expr.to_mongo()}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        # TODO: Convert tags to list of enums
        return cls.schema()


class MapLabels(EmbeddedModel):
    mapping: Dict[Shape, Dict[str, str]]

    def to_mongo(self):
        result = []

        for shape, mapping in self.mapping.items():
            field = _get_annotations_field(shape)
            expression = ViewField(field).map(
                ViewField().set_field("label", ViewField("label").map_values(mapping))
            )
            result.append({'$set': {field: expression.to_mongo()}})

        return result

    def validate_stage(self, project_labels: List[Label], **_):
        existing_mapping = {(label.name, label.shape) for label in project_labels}

        for shape, mapping in self.mapping.items():
            for label in mapping.keys():
                if (label, shape) not in existing_mapping:
                    raise ValueError(f'Label {label} with shape {shape} not found in project task')

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class SelectLabels(EmbeddedModel):
    labels: Dict[Shape, List[str]]
    filter_empty: bool = True

    def to_mongo(self):
        if not self.labels:
            return []

        project_result = {}
        is_empty_expr = ViewExpression(False)

        for shape in Shape:
            field = _get_annotations_field(shape)
            is_empty_expr = is_empty_expr | (ViewField(field).length() > 0)

        for shape, labels in self.labels.items():
            field = _get_annotations_field(shape)
            project_result[field] = ViewField(f'${field}').filter(ViewField('label').is_in(labels)).to_mongo()

        if not self.filter_empty:
            return [_get_projection_stage(project_result)]
        else:
            return [
                _get_projection_stage(project_result),
                {"$match": {"$expr": is_empty_expr.to_mongo()}}
            ]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, project_labels: List[Label], **_):
        labels_enum = Enum('Label', [(label.name, label.name) for label in project_labels])
        model = create_model('SelectLabels', labels=(List[labels_enum], ...), shape=(Shape, ...))
        return model.schema()


class FilterLabels(EmbeddedModel):
    filter: QueryExpression
    shape: Shape

    def to_mongo(self):
        field = _get_annotations_field(self.shape)
        filter_expr = construct_view_expression(self.filter)
        project = {field: ViewField(f'${field}').filter(filter_expr)}
        return [_get_projection_stage(project)]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class ExcludeLabels(EmbeddedModel):
    labels: Dict[Shape, List[str]]
    filter_empty: bool = True

    def to_mongo(self):
        project_result = {}
        is_empty_expr = ViewExpression(False)

        for shape in Shape:
            field = _get_annotations_field(shape)
            is_empty_expr = is_empty_expr | (ViewField(field).length() > 0)
            project_result[field] = 1

        for shape, labels in self.labels.items():
            field = _get_annotations_field(shape)
            project_result[field] = ViewField(f'${field}').filter(~ViewField('label').is_in(labels)).to_mongo()

        if not self.filter_empty:
            return [_get_projection_stage(project_result)]
        else:
            return [
                _get_projection_stage(project_result),
                {"$match": {"$expr": is_empty_expr.to_mongo()}}
            ]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, project_labels: List[Label], **_):
        labels_enum = Enum('Labels', [(label.name, label.name) for label in project_labels])
        model = create_model('ExcludeLabels', shape=(Shape, ...), labels=(List[labels_enum], ...))
        return model.schema()


class SelectAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterAttributes(
            attributes=self.attributes, filter=QueryExpression(literal=True)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterAttributes(attributes=self.attributes, filter=QueryExpression(literal=True)) \
            .validate_stage(*args, **kwargs)

    @classmethod
    def get_json_schema(cls, *args, **kwargs):
        return FilterAttributes.get_json_schema(*args, **kwargs)


class SelectLabelAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterLabelAttributes(
            attributes=self.attributes, filter=QueryExpression(literal=True)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterLabelAttributes(attributes=self.attributes, filter=QueryExpression(literal=True)) \
            .validate_stage(*args, **kwargs)

    @classmethod
    def get_json_schema(cls, *args, **kwargs):
        return FilterLabelAttributes.get_json_schema(*args, **kwargs)


class ExcludeLabelAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterLabelAttributes(attributes=self.attributes, filter=QueryExpression(literal=False)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterLabelAttributes(attributes=self.attributes, filter=QueryExpression(literal=False)) \
            .validate_stage(*args, **kwargs)

    @classmethod
    def get_json_schema(cls, *args, **kwargs):
        return FilterLabelAttributes.get_json_schema(*args, **kwargs)


class ExcludeAttributes(EmbeddedModel):
    attributes: List[str]

    def to_mongo(self):
        return FilterAttributes(attributes=self.attributes, filter=QueryExpression(literal=False)).to_mongo()

    def validate_stage(self, *args, **kwargs):
        FilterAttributes(attributes=self.attributes, filter=QueryExpression(literal=False)) \
            .validate_stage(*args, **kwargs)

    @classmethod
    def get_json_schema(cls, *args, **kwargs):
        return FilterAttributes.get_json_schema(*args, **kwargs)


class FilterAttributes(EmbeddedModel):
    attributes: List[str]
    filter: QueryExpression

    def to_mongo(self):
        if not self.attributes:
            return []

        filter_expr = construct_view_expression(self.filter).to_mongo()
        return {"$project": {f'attributes.{attribute}': filter_expr for attribute in self.attributes}}

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, project_attributes: List[str], **_):
        attributes_enum = Enum('Attribute', zip(project_attributes, project_attributes))
        model = create_model('FilterAttributes',
                             attributes=(List[attributes_enum], ...),
                             filter=(QueryExpression, ...))
        return model.schema()


class FilterLabelAttributes(EmbeddedModel):
    attributes: List[str]
    filter: QueryExpression
    shape: Optional[Shape]

    def to_mongo(self):
        if not self.attributes:
            return []

        shapes = [self.shape] if self.shape else list(Shape)
        result = {'$project': {}}
        expression = construct_view_expression(self.filter).to_mongo()

        for shape in shapes:
            field = _get_annotations_field(shape)
            result['$project'][field] = {'attributes': {attr: expression for attr in self.attributes}}

        return [result]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, project_attributes: List[str], **_):
        attributes_enum = Enum('Attribute', zip(project_attributes, project_attributes))
        model = create_model('FilterLabelAttributes',
                             attributes=(List[attributes_enum], ...),
                             filter=(QueryExpression, ...),
                             shape=(Optional[Shape], ...))
        return model.schema()


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

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


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

    @classmethod
    def get_json_schema(cls, project_labels: List[Label], **_):
        labels_enum = Enum('Label', [(label.name, label.name) for label in project_labels])
        model = create_model('LimitLabels', label=(labels_enum, ...), shape=(Shape, ...), limit=(int, ...))
        return model.schema()


class SetAttribute(EmbeddedModel):
    attribute: str
    expression: QueryExpression

    def to_mongo(self):
        expression = construct_view_expression(self.expression)
        expression = expression.to_mongo()
        return [{'$set': {f'{+ImageAnnotations.attributes}.{self.attribute}': expression}}]

    def validate_stage(self, **_):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class SetLabelAttribute(EmbeddedModel):
    shape: Shape
    label: str
    attribute: str
    expression: QueryExpression

    def to_mongo(self):
        expression = construct_view_expression(self.expression)
        expression = expression.to_mongo()
        field = _get_annotations_field(self.shape)
        set_field_expr = ViewField().set_field(f'attributes.{self.attribute}', expression)

        return [{
            "$project": {
                field: ViewField(f'${field}').map(set_field_expr)
            }
        }]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


def make_paginated_pipeline(pipeline: List[dict], page_size: int, page: int):
    stage = {
        '$facet': {
            'metadata': [{'$count': 'total'}, {'$addFields': {'page': page}}],
            'data': [{'$skip': page * page_size}, {'$limit': page_size}]
        }
    }
    return pipeline + [stage]


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
    'set_label_attribute': SetLabelAttribute,
    'exclude_label_attributes': ExcludeLabelAttributes,
    'select_label_attributes': SelectLabelAttributes,
    'filter_label_attributes': FilterLabelAttributes,
}
