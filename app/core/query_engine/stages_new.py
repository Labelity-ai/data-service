from typing import List, Optional
from abc import ABCMeta
import uuid
import random

from pymongo import ASCENDING, DESCENDING

from app.core.query_engine.expressions import ViewExpression, ViewField
from app.core.query_engine.builder import construct_view_expression
from app.models import Model, ModelConfig, ObjectId, EmbeddedModel, QueryExpression


class QueryStage(metaclass=ABCMeta):
    def get_filtered_list_fields(self):
        """Returns a list of names of fields or subfields that contain arrays
        that may have been filtered by the stage, if any.

        Returns:
            a list of fields, or ``None`` if no fields have been filtered
        """
        return None

    def get_selected_fields(self, frames=False):
        """Returns a list of fields that have been selected by the stage, if
        any.

        Args:
            frames (False): whether to return sample-level (False) or
                frame-level (True) fields

        Returns:
            a list of fields, or ``None`` if no fields have been selected
        """
        return None

    def get_excluded_fields(self, frames=False):
        """Returns a list of fields that have been excluded by the stage, if
        any.

        Args:
            frames (False): whether to return sample-level (False) or
                frame-level (True) fields

        Returns:
            a list of fields, or ``None`` if no fields have been selected
        """
        return None

    def to_mongo(self, sample_collection):
        """Returns the MongoDB aggregation pipeline for the stage.

        Args:
            sample_collection: the
                :class:`fiftyone.core.collections.SampleCollection` to which
                the stage is being applied

        Returns:
            a MongoDB aggregation pipeline (list of dicts)
        """
        raise NotImplementedError("subclasses must implement `to_mongo()`")

    def validate(self, sample_collection):
        """Validates that the stage can be applied to the given collection.

        Args:
            sample_collection: a
                :class:`fiftyone.core.collections.SampleCollection`

        Raises:
            :class:`ViewStageError`: if the stage cannot be applied to the
                collection
        """
        pass

    def _needs_frames(self, sample_collection):
        """Whether the stage requires frame labels of video samples to be
        attached.

        Args:
            sample_collection: the
                :class:`fiftyone.core.collections.SampleCollection` to which
                the stage is being applied

        Returns:
            True/False
        """
        return False

    def _serialize(self):
        """Returns a JSON dict representation of the :class:`ViewStage`.

        Returns:
            a JSON dict
        """
        if self._uuid is None:
            self._uuid = str(uuid.uuid4())

        return {
            "_cls": etau.get_class_name(self),
            "_uuid": self._uuid,
            "kwargs": self._kwargs(),
        }

    @classmethod
    def _params(self):
        """Returns a list of JSON dicts describing the parameters that define
        the stage.

        Returns:
            a list of JSON dicts
        """
        raise NotImplementedError("subclasses must implement `_params()`")


def _get_random_generator(seed):
    if seed is None:
        return random

    _random = random.Random()
    _random.seed(seed)
    return _random


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

        # TODO: Avoid creating new field here
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

        # TODO: Avoid creating new field here
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


class SetField(EmbeddedModel):
    field: str
    expression: QueryExpression

    def to_mongo(self, sample_collection, **_):
        return sample_collection._make_set_field_pipeline(
            self._field, self._expr, embedded_root=True
        )

    def _get_mongo_expr(self):
        if not isinstance(self.expression, ViewExpression):
            return self.expression

        # TODO: Doesnt't handle list fields
        if "." in self.field:
            prefix = "$" + self.field.rsplit(sep='.', maxsplit=1)[0]
        else:
            prefix = None

        return self.expression.to_mongo(prefix=prefix)

    def validate(self, sample_collection):
        sample_collection.validate_fields_exist(self.field)


class LimitLabels(EmbeddedModel):
    field: str
    limit: int
    _labels_list_field = None

    def to_mongo(self, sample_collection, **_):
        self._labels_list_field = _get_labels_list_field(
            sample_collection, self.field
        )

        limit = max(self.limit, 0)

        return [
            {
                "$set": {
                    self._labels_list_field: {
                        "$slice": ["$" + self._labels_list_field, limit]
                    }
                }
            }
        ]


def _get_labels_list_field(sample_collection, field_path):
    field, _ = _get_field(sample_collection, field_path)

    if isinstance(field, fof.EmbeddedDocumentField):
        document_type = field.document_type
        if issubclass(document_type, fol._HasLabelList):
            return field_path + "." + document_type._LABEL_LIST_FIELD

    raise ValueError(
        "Field '%s' must be a labels list type %s; found '%s'"
        % (field_path, fol._LABEL_LIST_FIELDS, field)
    )


def _get_field(sample_collection, field_path):
    field_name, is_frame_field = sample_collection._handle_frame_field(
        field_path
    )

    if is_frame_field:
        schema = sample_collection.get_frame_field_schema()
    else:
        schema = sample_collection.get_field_schema()

    if field_name not in schema:
        ftype = "Frame field" if is_frame_field else "Field"
        raise ValueError("%s '%s' does not exist" % (ftype, field_path))

    field = schema[field_name]

    return field, is_frame_field
