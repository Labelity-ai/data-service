from typing import List, Optional, Union, Dict, Any
import random
from datetime import datetime
from enum import Enum

from odmantic import Field
from pymongo import ASCENDING, DESCENDING
from aenum import extend_enum

from app.core.query_engine.expressions import ViewExpression, ViewField
from app.core.query_engine.builder import construct_view_expression
from app.models import ObjectId, EmbeddedModel, QueryExpression, ImageAnnotations, \
    Shape, Model, ModelConfig, Label
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


class Exclude(EmbeddedModel):
    """
    Exclude specific samples
    """

    samples: List[str]

    def to_mongo(self):
        return [{'$match': {+ImageAnnotations.event_id: {'$not': {'$in': self.samples}}}}]

    def validate_stage(self, *_, **__):
        pass

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Exists(EmbeddedModel):
    """
    Retrieve samples that has and specific field
    """

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
    """
    Limit the number of samples to retrieve
    """

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
    """
    Skip the N first samples
    """

    skip: int

    def to_mongo(self):
        if self.skip <= 0:
            return []

        return [{"$skip": self.skip}]

    @classmethod
    def get_json_schema(cls, **_):
        return cls.schema()


class Take(EmbeddedModel):
    """
    Retrieve a random sample
    """

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
    """
    Sort the samples randomly
    """

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
    """
    Select some specific samples
    """

    samples: List[str]

    def to_mongo(self):
        return [{'$match': {+ImageAnnotations.event_id: {'$in': self.samples}}}]

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
    """
    Label renaming. The labels that does not appear in the mapping dictionary will be passed through unmodified.
    """

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
    """
    Select some specific labels by name.
    If filter_empty is true, samples with no annotations after filtering will be discarded.
    """

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
    """
    Select annotations for each sample using an expression.
    The expression will be resolved for each annotation within the specific shape type,
    (every polygon, every tag, etc.), and if the expression resolves to true, the annotation will remain,
    otherwise, it will be discarded.
    """

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
    """
    Exclude annotations by label name.
    """

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
    """
    Select some specific image-level attributes and discard the rest.
    """

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
    """
    Select some specific annotation-level attributes and discard the rest.
    """

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
    """
    Exclude some specific annotation-level attributes and keep the rest.
    """

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
    """
    Exclude some specific image-level attributes and keep the rest.
    """

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
    """
    Filter some specific image-level attributes by expression. If the expression resolves to true, the
    attribute will remain, otherwise it will be discarded.
    """

    attributes: List[str]
    filter: QueryExpression

    def to_mongo(self):
        if not self.attributes:
            return []

        filter_expr = construct_view_expression(self.filter).to_mongo()
        project_expr = {f'attributes.{attribute}': filter_expr for attribute in self.attributes}
        return [_get_projection_stage(project_expr)]

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
    """
    Filter some specific annotation-level attributes by expression. If the expression resolves to true, the
    attribute will remain, otherwise it will be discarded.
    """

    attributes: List[str]
    filter: QueryExpression
    shape: Optional[Shape]

    def to_mongo(self):
        if not self.attributes:
            return []

        shapes = [self.shape] if self.shape else list(Shape)
        result = {}

        for shape in shapes:
            field = _get_annotations_field(shape)

            for attr in self.attributes:
                attr_field = f'attributes.{attr}'
                filter_expr = self.filter.copy()
                filter_expr.field = f'${result[field]}.attributes.{attr_field}{filter_expr.field}'
                expression = construct_view_expression(filter_expr).to_mongo()
                result[field][attr_field] = {
                    '$cond': {
                        'if': expression,
                        'then': f'${result[field]}.attributes.{attr_field}',
                        'else': '$$REMOVE',
                    }
                }

        return [_get_projection_stage(result)]

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
    """
    Sort the samples by field or expression.
    The expression should resolve to something sortable (bool, str or number)
    """

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
    """
    Limit the number of boxes, polygons, tags, or points for an specific label.
    """

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
                    '$concatArrays': [{"$slice": [labels_expr, 0, limit]}, other_labels_expr]
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


class SkipLabels(EmbeddedModel):
    """
    Skip the first N boxes, polygons, tags, or points for an specific label.
    """

    label: str
    shape: Shape
    skip: int

    def to_mongo(self):
        skip = max(self.skip, 0)
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
                    '$concatArrays': [{"$slice": [labels_expr, skip]}, other_labels_expr]
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
    """
    Add or replace the value of an image-level attribute.
    """

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
    """
    Add or replace the value of an annotation-level attribute.
    """

    shape: Shape
    label: str
    attribute: str
    expression: QueryExpression

    def to_mongo(self):
        expression = construct_view_expression(self.expression)
        expression = expression.to_mongo()
        field = _get_annotations_field(self.shape)
        set_field_expr = ViewField().set_field(f'attributes.{self.attribute}', expression)
        project_expr = {field: ViewField(f'${field}').map(set_field_expr)}
        return [_get_projection_stage(project_expr)]

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


class StagesEnum(str, Enum):
    pass


for stage in STAGES.keys():
    extend_enum(StagesEnum, stage, stage)


class QueryStage(EmbeddedModel):
    stage: StagesEnum
    parameters: Dict[str, Any]

    @root_validator
    def validate_root(cls, values):
        parameters = values['parameters']
        stage = values['stage'].value
        STAGES[stage](**parameters)
        return values


class QueryPipeline(Model):
    steps: List[QueryStage]
    project_id: ObjectId
    created_at: datetime = Field(default_factory=datetime.utcnow)
    dataset_id: Optional[ObjectId] = None

    Config = ModelConfig
