from collections import defaultdict
import random
import reprlib
import uuid
import warnings

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING

import eta.core.utils as etau

from app.core.query_engine import expressions

import fiftyone.core.fields as fof
import fiftyone.core.labels as fol
import fiftyone.core.media as fom
import fiftyone.core.sample as fos
from fiftyone.core.odm.document import MongoEngineBaseDocument
from fiftyone.core.odm.frame import DatasetFrameSampleDocument
from fiftyone.core.odm.mixins import default_sample_fields
from fiftyone.core.odm.sample import DatasetSampleDocument


class ViewStage(object):
    """Abstract base class for all view stages.

    :class:`ViewStage` instances represent logical operations to apply to
    :class:`fiftyone.core.collections.SampleCollection` instances, which may
    decide what subset of samples in the collection should pass though the
    stage, and also what subset of the contents of each
    :class:`fiftyone.core.sample.Sample` should be passed. The output of
    view stages are represented by a :class:`fiftyone.core.view.DatasetView`.
    """

    _uuid = None

    def __str__(self):
        return repr(self)

    def __repr__(self):
        kwargs_list = []
        for k, v in self._kwargs():
            if k.startswith("_"):
                continue

            v_repr = _repr.repr(v)
            # v_repr = etau.summarize_long_str(v_repr, 30)
            kwargs_list.append("%s=%s" % (k, v_repr))

        kwargs_str = ", ".join(kwargs_list)
        return "%s(%s)" % (self.__class__.__name__, kwargs_str)

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

    def _kwargs(self):
        """Returns a list of ``[name, value]`` lists describing the parameters
        that define the stage.

        Returns:
            a JSON dict
        """
        raise NotImplementedError("subclasses must implement `_kwargs()`")

    @classmethod
    def _params(self):
        """Returns a list of JSON dicts describing the parameters that define
        the stage.

        Returns:
            a list of JSON dicts
        """
        raise NotImplementedError("subclasses must implement `_params()`")

    @classmethod
    def _from_dict(cls, d):
        """Creates a :class:`ViewStage` instance from a serialized JSON dict
        representation of it.

        Args:
            d: a JSON dict

        Returns:
            a :class:`ViewStage`
        """
        view_stage_cls = etau.get_class(d["_cls"])
        uuid = d.get("_uuid", None)
        stage = view_stage_cls(**{k: v for (k, v) in d["kwargs"]})
        stage._uuid = uuid
        return stage


class ViewStageError(Exception):
    """An error raised when a problem with a :class:`ViewStage` is encountered.
    """

    pass


class ExcludeFields(ViewStage):
    """Excludes the fields with the given names from the samples in a
    collection.

    Note that default fields cannot be excluded.

    Examples::

        import fiftyone as fo

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    ground_truth=fo.Classification(label="cat"),
                    predictions=fo.Classification(label="cat", confidence=0.9),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    ground_truth=fo.Classification(label="dog"),
                    predictions=fo.Classification(label="dog", confidence=0.8),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    ground_truth=None,
                    predictions=None,
                ),
            ]
        )

        #
        # Exclude the `predictions` field from all samples
        #

        stage = fo.ExcludeFields("predictions")
        view = dataset.add_stage(stage)

    Args:
        field_names: a field name or iterable of field names to exclude
    """

    def __init__(self, field_names):
        if etau.is_str(field_names):
            field_names = [field_names]
        else:
            field_names = list(field_names)

        self._field_names = field_names
        self._dataset = None

    @property
    def field_names(self):
        """The list of field names to exclude."""
        return self._field_names

    def get_excluded_fields(self, frames=False):
        if frames:
            default_fields = default_sample_fields(
                DatasetFrameSampleDocument, include_private=True
            )
            excluded_fields = [
                f[len(self._dataset._FRAMES_PREFIX) :]
                for f in self.field_names
                if f.startswith(self._dataset._FRAMES_PREFIX)
            ]
        else:
            default_fields = default_sample_fields(
                DatasetSampleDocument, include_private=True
            )
            if not frames and (self._dataset.media_type == fom.VIDEO):
                default_fields += ("frames",)

            excluded_fields = [
                f
                for f in self.field_names
                if not f.startswith(self._dataset._FRAMES_PREFIX)
            ]

        for field_name in excluded_fields:
            if field_name.startswith("_"):
                raise ValueError(
                    "Cannot exclude private field '%s'" % field_name
                )

            if field_name in default_fields:
                raise ValueError(
                    "Cannot exclude default field '%s'" % field_name
                )

        return excluded_fields

    def to_mongo(self, _, **__):
        fields = self.get_excluded_fields(
            frames=False
        ) + self.get_excluded_fields(frames=True)
        if not fields:
            return []

        return [{"$unset": fields}]

    def _kwargs(self):
        return [["field_names", self._field_names]]

    @classmethod
    def _params(self):
        return [
            {
                "name": "field_names",
                "type": "list<str>",
                "placeholder": "list,of,fields",
            }
        ]

    def validate(self, sample_collection):
        # Using dataset here allows a field to be excluded multiple times
        self._dataset = sample_collection._dataset
        self._dataset.validate_fields_exist(self.field_names)


class ExcludeObjects(ViewStage):
    """Excludes the specified objects from a collection.

    The returned view will omit the objects specified in the provided
    ``objects`` argument, which should have the following format::

        [
            {
                "sample_id": "5f8d254a27ad06815ab89df4",
                "field": "ground_truth",
                "object_id": "5f8d254a27ad06815ab89df3",
            },
            {
                "sample_id": "5f8d255e27ad06815ab93bf8",
                "field": "ground_truth",
                "object_id": "5f8d255e27ad06815ab93bf6",
            },
            ...
        ]

    Examples::

        import fiftyone as fo
        import fiftyone.zoo as foz

        dataset = foz.load_zoo_dataset("quickstart")

        #
        # Exclude the objects currently selected in the App
        #

        session = fo.launch_app(dataset)

        # Select some objects in the App...

        stage = fo.ExcludeObjects(session.selected_objects)
        view = dataset.add_stage(stage)

    Args:
        objects: a list of dicts specifying the objects to exclude
    """

    def __init__(self, objects):
        _, object_ids = _parse_objects(objects)
        self._objects = objects
        self._object_ids = object_ids
        self._pipeline = None

    @property
    def objects(self):
        """A list of dicts specifying the objects to exclude."""
        return self._objects

    def to_mongo(self, _, **__):
        if self._pipeline is None:
            raise ValueError(
                "`validate()` must be called before using a %s stage"
                % self.__class__
            )

        return self._pipeline

    def _kwargs(self):
        return [["objects", self._objects]]

    @classmethod
    def _params(self):
        return [
            {
                "name": "objects",
                "type": "dict",  # @todo use "list<dict>" when supported
                "placeholder": "[{...}]",
            }
        ]

    def _make_pipeline(self, sample_collection):
        label_schema = sample_collection.get_field_schema(
            ftype=fof.EmbeddedDocumentField, embedded_doc_type=fol.Label
        )

        pipeline = []
        for field, object_ids in self._object_ids.items():
            label_filter = ~expressions.ViewField("_id").is_in(
                [expressions.ObjectId(oid) for oid in object_ids]
            )
            stage = _make_label_filter_stage(label_schema, field, label_filter)
            if stage is None:
                continue

            stage.validate(sample_collection)
            pipeline.extend(stage.to_mongo(sample_collection))

        return pipeline

    def validate(self, sample_collection):
        self._pipeline = self._make_pipeline(sample_collection)


class FilterField(ViewStage):
    """Filters the values of a given sample (or embedded document) field of
    each sample in a collection.

    Values of ``field`` for which ``filter`` returns ``False`` are
    replaced with ``None``.

    Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    ground_truth=fo.Classification(label="cat"),
                    predictions=fo.Classification(label="cat", confidence=0.9),
                    numeric_field=1.0,
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    ground_truth=fo.Classification(label="dog"),
                    predictions=fo.Classification(label="dog", confidence=0.8),
                    numeric_field=-1.0,
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    ground_truth=None,
                    predictions=None,
                    numeric_field=None,
                ),
            ]
        )

        #
        # Only include classifications in the `predictions` field
        # whose `label` is "cat"
        #

        stage = fo.FilterField("predictions", F("label") == "cat")
        view = dataset.add_stage(stage)

        #
        # Only include samples whose `numeric_field` value is positive
        #

        stage = fo.FilterField("numeric_field", F() > 0)
        view = dataset.add_stage(stage)

    Args:
        field: the name of the field to filter
        filter: a :class:`fiftyone.core.expressions.ViewExpression` or
            `MongoDB expression <https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#aggregation-expressions>`_
            that returns a boolean describing the filter to apply
        only_matches (True): whether to only include samples that match the
            filter (True) or include all samples (False)
    """

    def __init__(self, field, filter, only_matches=True):
        self._field = field
        self._filter = filter
        self._hide_result = False
        self._only_matches = only_matches
        self._is_frame_field = None
        self._validate_params()

    @property
    def field(self):
        """The field to filter."""
        return self._field

    @property
    def filter(self):
        """The filter expression."""
        return self._filter

    @property
    def only_matches(self):
        """Whether to only include samples that match the filter."""
        return self._only_matches

    def to_mongo(self, sample_collection):
        field_name, is_frame_field = sample_collection._handle_frame_field(
            self._field
        )
        new_field = self._get_new_field(sample_collection)
        if is_frame_field:
            return _get_filter_frames_field_pipeline(
                field_name,
                new_field,
                self._filter,
                only_matches=self._only_matches,
            )

        return _get_filter_field_pipeline(
            field_name,
            new_field,
            self._filter,
            only_matches=self._only_matches,
            hide_result=self._hide_result,
        )

    def _get_mongo_filter(self):
        if self._is_frame_field:
            filter_field = self._field.split(".", 1)[1]  # remove `frames`
            return _get_field_mongo_filter(
                self._filter, prefix="$frame." + filter_field
            )

        return _get_field_mongo_filter(self._filter, prefix=self._field)

    def _get_new_field(self, sample_collection):
        field, _ = sample_collection._handle_frame_field(self._field)

        if self._hide_result:
            return "__" + field

        return field

    def _needs_frames(self, sample_collection):
        return sample_collection._handle_frame_field(self._field)[1]

    def _kwargs(self):
        return [
            ["field", self._field],
            ["filter", self._get_mongo_filter()],
            ["only_matches", self._only_matches],
        ]

    @classmethod
    def _params(self):
        return [
            {"name": "field", "type": "field"},
            {"name": "filter", "type": "dict", "placeholder": ""},
            {
                "name": "only_matches",
                "type": "bool",
                "default": "True",
                "placeholder": "only matches (default=True)",
            },
        ]

    def _validate_params(self):
        if not isinstance(self._filter, (expressions.ViewExpression, dict)):
            raise ValueError(
                "Filter must be a ViewExpression or a MongoDB expression; "
                "found '%s'" % self._filter
            )

    def validate(self, sample_collection):
        if self._field == "filepath":
            raise ValueError("Cannot filter required field `filepath`")

        sample_collection.validate_fields_exist(self._field)

        _, is_frame_field = sample_collection._handle_frame_field(self._field)
        self._is_frame_field = is_frame_field


def _get_filter_field_pipeline(
        filter_field, new_field, filter_arg, only_matches=True, hide_result=False
):
    cond = _get_field_mongo_filter(filter_arg, prefix=filter_field)

    pipeline = [
        {
            "$set": {
                new_field: {
                    "$cond": {
                        "if": cond,
                        "then": "$" + filter_field,
                        "else": None,
                    }
                }
            }
        }
    ]

    if only_matches:
        pipeline.append(
            {"$match": {new_field: {"$exists": True, "$ne": None}}}
        )

    if hide_result:
        pipeline.append({"$unset": new_field})

    return pipeline


def _get_filter_frames_field_pipeline(
        filter_field, new_field, filter_arg, only_matches=True, hide_result=False,
):
    cond = _get_field_mongo_filter(filter_arg, prefix="$frame." + filter_field)

    pipeline = [
        {
            "$set": {
                "frames": {
                    "$map": {
                        "input": "$frames",
                        "as": "frame",
                        "in": {
                            "$mergeObjects": [
                                "$$frame",
                                {
                                    new_field: {
                                        "$cond": {
                                            "if": cond,
                                            "then": "$$frame." + filter_field,
                                            "else": None,
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    if only_matches:
        pipeline.append(
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {
                                "$reduce": {
                                    "input": "$frames",
                                    "initialValue": 0,
                                    "in": {
                                        "$sum": [
                                            "$$value",
                                            {
                                                "$cond": [
                                                    {
                                                        "$ne": [
                                                            "$$this."
                                                            + new_field,
                                                            None,
                                                            ]
                                                    },
                                                    1,
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                }
                            },
                            0,
                        ]
                    }
                }
            }
        )

    if hide_result:
        pipeline.append({"$unset": "frames." + new_field})

    return pipeline


def _get_field_mongo_filter(filter_arg, prefix="$this"):
    if isinstance(filter_arg, expressions.ViewExpression):
        return filter_arg.to_mongo(prefix="$" + prefix)

    return filter_arg


class FilterLabels(FilterField):
    """Filters the :class:`fiftyone.core.labels.Label` field of each sample in
    a collection.

    If the specified ``field`` is a single :class:`fiftyone.core.labels.Label`
    type, fields for which ``filter`` returns ``False`` are replaced with
    ``None``:

    -   :class:`fiftyone.core.labels.Classification`
    -   :class:`fiftyone.core.labels.Detection`
    -   :class:`fiftyone.core.labels.Polyline`
    -   :class:`fiftyone.core.labels.Keypoint`

    If the specified ``field`` is a :class:`fiftyone.core.labels.Label` list
    type, the label elements for which ``filter`` returns ``False`` are omitted
    from the view:

    -   :class:`fiftyone.core.labels.Classifications`
    -   :class:`fiftyone.core.labels.Detections`
    -   :class:`fiftyone.core.labels.Polylines`
    -   :class:`fiftyone.core.labels.Keypoints`

    Classifications Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    predictions=fo.Classification(label="cat", confidence=0.9),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    predictions=fo.Classification(label="dog", confidence=0.8),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    predictions=fo.Classification(label="rabbit"),
                ),
                fo.Sample(
                    filepath="/path/to/image4.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Only include classifications in the `predictions` field whose
        # `confidence` is greater than 0.8
        #

        stage = fo.FilterLabels("predictions", F("confidence") > 0.8)
        view = dataset.add_stage(stage)

        #
        # Only include classifications in the `predictions` field whose `label`
        # is "cat" or "dog"
        #

        stage = fo.FilterLabels("predictions", F("label").is_in(["cat", "dog"]))
        view = dataset.add_stage(stage)

    Detections Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.1, 0.1, 0.5, 0.5],
                                confidence=0.9,
                            ),
                            fo.Detection(
                                label="dog",
                                bounding_box=[0.2, 0.2, 0.3, 0.3],
                                confidence=0.8,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.5, 0.5, 0.4, 0.4],
                                confidence=0.95,
                            ),
                            fo.Detection(label="rabbit"),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="squirrel",
                                bounding_box=[0.25, 0.25, 0.5, 0.5],
                                confidence=0.5,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image4.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Only include detections in the `predictions` field whose `confidence`
        # is greater than 0.8
        #

        stage = fo.FilterLabels("predictions", F("confidence") > 0.8)
        view = dataset.add_stage(stage)

        #
        # Only include detections in the `predictions` field whose `label` is
        # "cat" or "dog"
        #

        stage = fo.FilterLabels("predictions", F("label").is_in(["cat", "dog"]))
        view = dataset.add_stage(stage)

        #
        # Only include detections in the `predictions` field whose bounding box
        # area is smaller than 0.2
        #

        # Bboxes are in [top-left-x, top-left-y, width, height] format
        bbox_area = F("bounding_box")[2] * F("bounding_box")[3]

        stage = fo.FilterLabels("predictions", bbox_area < 0.2)
        view = dataset.add_stage(stage)

    Polylines Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    predictions=fo.Polylines(
                        polylines=[
                            fo.Polyline(
                                label="lane",
                                points=[[(0.1, 0.1), (0.1, 0.6)]],
                                filled=False,
                            ),
                            fo.Polyline(
                                label="road",
                                points=[[(0.2, 0.2), (0.5, 0.5), (0.2, 0.5)]],
                                filled=True,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    predictions=fo.Polylines(
                        polylines=[
                            fo.Polyline(
                                label="lane",
                                points=[[(0.4, 0.4), (0.9, 0.4)]],
                                filled=False,
                            ),
                            fo.Polyline(
                                label="road",
                                points=[[(0.6, 0.6), (0.9, 0.9), (0.6, 0.9)]],
                                filled=True,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Only include polylines in the `predictions` field that are filled
        #

        stage = fo.FilterLabels("predictions", F("filled") == True)
        view = dataset.add_stage(stage)

        #
        # Only include polylines in the `predictions` field whose `label` is
        # "lane"
        #

        stage = fo.FilterLabels("predictions", F("label") == "lane")
        view = dataset.add_stage(stage)

        #
        # Only include polylines in the `predictions` field with at least
        # 3 vertices
        #

        num_vertices = F("points").map(F().length()).sum()
        stage = fo.FilterLabels("predictions", num_vertices >= 3)
        view = dataset.add_stage(stage)

    Keypoints Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    predictions=fo.Keypoint(
                        label="house",
                        points=[(0.1, 0.1), (0.1, 0.9), (0.9, 0.9), (0.9, 0.1)],
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    predictions=fo.Keypoint(
                        label="window",
                        points=[(0.4, 0.4), (0.5, 0.5), (0.6, 0.6)],
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Only include keypoints in the `predictions` field whose `label` is
        # "house"
        #

        stage = fo.FilterLabels("predictions", F("label") == "house")
        view = dataset.add_stage(stage)

        #
        # Only include keypoints in the `predictions` field with less than four
        # points
        #

        stage = fo.FilterLabels("predictions", F("points").length() < 4)
        view = dataset.add_stage(stage)

    Args:
        field: the labels field to filter
        filter: a :class:`fiftyone.core.expressions.ViewExpression` or
            `MongoDB expression <https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#aggregation-expressions>`_
            that returns a boolean describing the filter to apply
        only_matches (True): whether to only include samples with at least
            one label after filtering (True) or include all samples (False)
    """

    def __init__(self, field, filter, only_matches=True):
        self._field = field
        self._filter = filter
        self._only_matches = only_matches
        self._hide_result = False
        self._labels_field = None
        self._is_frame_field = None
        self._is_labels_list_field = None
        self._is_frame_field = None
        self._validate_params()

    def get_filtered_list_fields(self):
        if self._is_labels_list_field:
            return [self._labels_field]

        return None

    def to_mongo(self, sample_collection):
        self._get_labels_field(sample_collection)

        labels_field, is_frame_field = sample_collection._handle_frame_field(
            self._labels_field
        )
        new_field = self._get_new_field(sample_collection)

        if is_frame_field:
            if self._is_labels_list_field:
                _make_pipeline = _get_filter_frames_list_field_pipeline
            else:
                _make_pipeline = _get_filter_frames_field_pipeline
        elif self._is_labels_list_field:
            _make_pipeline = _get_filter_list_field_pipeline
        else:
            _make_pipeline = _get_filter_field_pipeline

        return _make_pipeline(
            labels_field,
            new_field,
            self._filter,
            only_matches=self._only_matches,
            hide_result=self._hide_result,
        )

    def _needs_frames(self, sample_collection):
        return sample_collection._handle_frame_field(self._labels_field)[1]

    def _get_mongo_filter(self):
        if self._is_labels_list_field:
            return _get_list_field_mongo_filter(self._filter)

        if self._is_frame_field:
            filter_field = self._field.split(".", 1)[1]  # remove `frames`
            return _get_field_mongo_filter(
                self._filter, prefix="$frame." + filter_field
            )

        return _get_field_mongo_filter(self._filter, prefix=self._field)

    def _get_labels_field(self, sample_collection):
        field_name, is_list_field, is_frame_field = _get_labels_field(
            sample_collection, self._field
        )
        self._is_frame_field = is_frame_field
        self._labels_field = field_name
        self._is_labels_list_field = is_list_field
        self._is_frame_field = is_frame_field

    def _get_new_field(self, sample_collection):
        field, _ = sample_collection._handle_frame_field(self._labels_field)

        if self._hide_result:
            return "__%s" % field

        return field

    def validate(self, sample_collection):
        self._get_labels_field(sample_collection)


def _get_filter_list_field_pipeline(
        filter_field, new_field, filter_arg, only_matches=True, hide_result=False
):
    cond = _get_list_field_mongo_filter(filter_arg)

    pipeline = [
        {
            "$set": {
                filter_field: {
                    "$filter": {"input": "$" + filter_field, "cond": cond}
                }
            }
        }
    ]

    if only_matches:
        pipeline.append(
            {
                "$match": {
                    filter_field: {
                        "$gt": [
                            {"$size": {"$ifNull": ["$" + filter_field, []]}},
                            0,
                        ]
                    }
                }
            }
        )

    if hide_result:
        pipeline.append({"$unset": new_field})

    return pipeline


def _get_filter_frames_list_field_pipeline(
        filter_field, new_field, filter_arg, only_matches=True, hide_result=False,
):
    cond = _get_list_field_mongo_filter(filter_arg)
    label_field, labels_list = new_field.split(".")

    pipeline = [
        {
            "$set": {
                "frames": {
                    "$map": {
                        "input": "$frames",
                        "as": "frame",
                        "in": {
                            "$mergeObjects": [
                                "$$frame",
                                {
                                    label_field: {
                                        "$mergeObjects": [
                                            "$$frame." + label_field,
                                            {
                                                labels_list: {
                                                    "$filter": {
                                                        "input": "$$frame."
                                                                 + filter_field,
                                                        "cond": cond,
                                                    }
                                                }
                                            },
                                            ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    if only_matches:
        pipeline.append(
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {
                                "$reduce": {
                                    "input": "$frames",
                                    "initialValue": 0,
                                    "in": {
                                        "$sum": [
                                            "$$value",
                                            {
                                                "$size": {
                                                    "$filter": {
                                                        "input": "$$this."
                                                                 + new_field,
                                                        "cond": cond,
                                                    }
                                                }
                                            },
                                        ]
                                    },
                                }
                            },
                            0,
                        ]
                    }
                }
            }
        )

    if hide_result:
        pipeline.append({"$unset": "frames." + new_field})

    return pipeline


def _get_list_field_mongo_filter(filter_arg, prefix="$this"):
    if isinstance(filter_arg, expressions.ViewExpression):
        return filter_arg.to_mongo(prefix="$" + prefix)

    return filter_arg


class _FilterListField(FilterField):
    def _get_new_field(self, sample_collection):
        field = self._filter_field
        if self._needs_frames(sample_collection):
            field = field.split(".", 1)[1]  # remove `frames`

        if self._hide_result:
            return "__" + field

        return field

    @property
    def _filter_field(self):
        raise NotImplementedError("subclasses must implement `_filter_field`")

    def get_filtered_list_fields(self):
        return [self._filter_field]

    def to_mongo(self, sample_collection):
        filter_field, is_frame_field = sample_collection._handle_frame_field(
            self._filter_field
        )
        new_field = self._get_new_field(sample_collection)

        if is_frame_field:
            _make_pipeline = _get_filter_frames_list_field_pipeline
        else:
            _make_pipeline = _get_filter_list_field_pipeline

        return _make_pipeline(
            filter_field,
            new_field,
            self._filter,
            only_matches=self._only_matches,
            hide_result=self._hide_result,
        )

    def _get_mongo_filter(self):
        return _get_list_field_mongo_filter(self._filter)

    def validate(self, sample_collection):
        raise NotImplementedError("subclasses must implement `validate()`")


class MapLabels(ViewStage):
    """Maps the ``label`` values of a :class:`fiftyone.core.labels.Label` field
    to new values for each sample in a collection.

    Examples::

        import fiftyone as fo
        from fiftyone import ViewField as F

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    weather=fo.Classification(label="sunny"),
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.1, 0.1, 0.5, 0.5],
                                confidence=0.9,
                            ),
                            fo.Detection(
                                label="dog",
                                bounding_box=[0.2, 0.2, 0.3, 0.3],
                                confidence=0.8,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    weather=fo.Classification(label="cloudy"),
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.5, 0.5, 0.4, 0.4],
                                confidence=0.95,
                            ),
                            fo.Detection(label="rabbit"),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    weather=fo.Classification(label="partly cloudy"),
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="squirrel",
                                bounding_box=[0.25, 0.25, 0.5, 0.5],
                                confidence=0.5,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image4.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Map the "partly cloudy" weather label to "cloudy"
        #

        stage = fo.MapLabels("weather", {"partly cloudy": "cloudy"})
        view = dataset.add_stage(stage)

        #
        # Map "rabbit" and "squirrel" predictions to "other"
        #

        stage = fo.MapLabels(
            "predictions", {"rabbit": "other", "squirrel": "other"}
        )
        view = dataset.add_stage(stage)

    Args:
        field: the labels field to map
        map: a dict mapping label values to new label values
    """

    def __init__(self, field, map):
        self._field = field
        self._map = map
        self._labels_field = None

    @property
    def field(self):
        """The labels field to map."""
        return self._field

    @property
    def map(self):
        """The labels map dict."""
        return self._map

    def to_mongo(self, sample_collection, **_):
        labels_field, _, is_frame_field = _get_labels_field(
            sample_collection, self._field
        )

        label_path = labels_field + ".label"
        expr = expressions.ViewField().map_values(self._map)
        return sample_collection._make_set_field_pipeline(label_path, expr)

    def _kwargs(self):
        return [
            ["field", self._field],
            ["map", self._map],
        ]

    @classmethod
    def _params(cls):
        return [
            {"name": "field", "type": "field"},
            {"name": "map", "type": "dict", "placeholder": "map"},
        ]

    def validate(self, sample_collection):
        _get_labels_field(sample_collection, self._field)


class MatchTags(ViewStage):
    """Returns a view containing the samples in the collection that have any of
    the given tag(s).

    To match samples that must contain multiple tags, chain multiple
    :class:`MatchTags` stages together.

    Examples::

        import fiftyone as fo

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    tags=["train"],
                    ground_truth=fo.Classification(label="cat"),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    tags=["test"],
                    ground_truth=fo.Classification(label="cat"),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    ground_truth=None,
                ),
            ]
        )

        #
        # Only include samples that have the "test" tag
        #

        stage = fo.MatchTags("test")
        view = dataset.add_stage(stage)

        #
        # Only include samples that have either the "test" or "train" tag
        #

        stage = fo.MatchTags(["test", "train"])
        view = dataset.add_stage(stage)

    Args:
        tags: the tag or iterable of tags to match
    """

    def __init__(self, tags):
        if etau.is_str(tags):
            tags = [tags]
        else:
            tags = list(tags)

        self._tags = tags

    @property
    def tags(self):
        """The list of tags to match."""
        return self._tags

    def to_mongo(self, _, **__):
        return [{"$match": {"tags": {"$in": self._tags}}}]

    def _kwargs(self):
        return [["tags", self._tags]]

    @classmethod
    def _params(cls):
        return [
            {
                "name": "tags",
                "type": "list<str>",
                "placeholder": "list,of,tags",
            }
        ]


class Mongo(ViewStage):
    """A view stage defined by a raw MongoDB aggregation pipeline.

    See `MongoDB aggregation pipelines <https://docs.mongodb.com/manual/core/aggregation-pipeline/>`_
    for more details.

    Examples::

        import fiftyone as fo

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.1, 0.1, 0.5, 0.5],
                                confidence=0.9,
                            ),
                            fo.Detection(
                                label="dog",
                                bounding_box=[0.2, 0.2, 0.3, 0.3],
                                confidence=0.8,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="cat",
                                bounding_box=[0.5, 0.5, 0.4, 0.4],
                                confidence=0.95,
                            ),
                            fo.Detection(label="rabbit"),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    predictions=fo.Detections(
                        detections=[
                            fo.Detection(
                                label="squirrel",
                                bounding_box=[0.25, 0.25, 0.5, 0.5],
                                confidence=0.5,
                            ),
                        ]
                    ),
                ),
                fo.Sample(
                    filepath="/path/to/image4.png",
                    predictions=None,
                ),
            ]
        )

        #
        # Extract a view containing the second and third samples in the dataset
        #

        stage = fo.Mongo([{"$skip": 1}, {"$limit": 2}])
        view = dataset.add_stage(stage)

        #
        # Sort by the number of objects in the `precictions` field
        #

        stage = fo.Mongo([
            {
                "$set": {
                    "_sort_field": {
                        "$size": {"$ifNull": ["$predictions.detections", []]}
                    }
                }
            },
            {"$sort": {"_sort_field": -1}},
            {"$unset": "_sort_field"}
        ])
        view = dataset.add_stage(stage)

    Args:
        pipeline: a MongoDB aggregation pipeline (list of dicts)
    """

    def __init__(self, pipeline):
        self._pipeline = pipeline

    @property
    def pipeline(self):
        """The MongoDB aggregation pipeline."""
        return self._pipeline

    def to_mongo(self, _, **__):
        return self._pipeline

    def _kwargs(self):
        return [["pipeline", self._pipeline]]

    @classmethod
    def _params(self):
        return [{"name": "pipeline", "type": "dict", "placeholder": ""}]


class SelectFields(ViewStage):
    """Selects only the fields with the given names from the samples in the
    collection. All other fields are excluded.

    Note that default sample fields are always selected.

    Examples::

        import fiftyone as fo

        dataset = fo.Dataset()
        dataset.add_samples(
            [
                fo.Sample(
                    filepath="/path/to/image1.png",
                    numeric_field=1.0,
                    numeric_list_field=[-1, 0, 1],
                ),
                fo.Sample(
                    filepath="/path/to/image2.png",
                    numeric_field=-1.0,
                    numeric_list_field=[-2, -1, 0, 1],
                ),
                fo.Sample(
                    filepath="/path/to/image3.png",
                    numeric_field=None,
                ),
            ]
        )

        #
        # Include only the default fields on each sample
        #

        stage = fo.SelectFields()
        view = dataset.add_stage(stage)

        #
        # Include only the `numeric_field` field (and the default fields) on
        # each sample
        #

        stage = fo.SelectFields("numeric_field")
        view = dataset.add_stage(stage)

    Args:
        field_names (None): a field name or iterable of field names to select
    """

    def __init__(self, field_names=None):
        if etau.is_str(field_names):
            field_names = [field_names]
        elif field_names:
            field_names = list(field_names)

        self._field_names = field_names
        self._dataset = None

    @property
    def field_names(self):
        """The list of field names to select."""
        return self._field_names or []

    def get_selected_fields(self, frames=False):
        if frames:
            default_fields = default_sample_fields(
                DatasetFrameSampleDocument, include_private=True
            )

            selected_fields = [
                f[len(self._dataset._FRAMES_PREFIX) :]
                for f in self.field_names
                if f.startswith(self._dataset._FRAMES_PREFIX)
            ]
        else:
            default_fields = default_sample_fields(
                DatasetSampleDocument, include_private=True
            )
            if not frames and (self._dataset.media_type == fom.VIDEO):
                default_fields += ("frames",)

            selected_fields = [
                f
                for f in self.field_names
                if not f.startswith(self._dataset._FRAMES_PREFIX)
            ]

        return list(set(selected_fields) | set(default_fields))

    def to_mongo(self, _, **__):
        selected_fields = self.get_selected_fields(
            frames=False
        ) + self.get_selected_fields(frames=True)
        if not selected_fields:
            return []

        return [{"$project": {fn: True for fn in selected_fields}}]

    def _kwargs(self):
        return [["field_names", self._field_names]]

    @classmethod
    def _params(self):
        return [
            {
                "name": "field_names",
                "type": "NoneType|list<str>",
                "default": "None",
                "placeholder": "list,of,fields",
            }
        ]

    def _validate_params(self):
        for field_name in self.field_names:
            if field_name.startswith("_"):
                raise ValueError(
                    "Cannot select private field '%s'" % field_name
                )

    def validate(self, sample_collection):
        self._dataset = sample_collection._dataset
        sample_collection.validate_fields_exist(self.field_names)


class SelectObjects(ViewStage):
    """Selects only the specified objects from a collection.

    The returned view will omit samples, sample fields, and individual objects
    that do not appear in the provided ``objects`` argument, which should have
    the following format::

        [
            {
                "sample_id": "5f8d254a27ad06815ab89df4",
                "field": "ground_truth",
                "object_id": "5f8d254a27ad06815ab89df3",
            },
            {
                "sample_id": "5f8d255e27ad06815ab93bf8",
                "field": "ground_truth",
                "object_id": "5f8d255e27ad06815ab93bf6",
            },
            ...
        ]

    Examples::

        import fiftyone as fo
        import fiftyone.zoo as foz

        dataset = foz.load_zoo_dataset("quickstart")

        #
        # Only include the objects currently selected in the App
        #

        session = fo.launch_app(dataset)

        # Select some objects in the App...

        stage = fo.SelectObjects(session.selected_objects)
        view = dataset.add_stage(stage)

    Args:
        objects: a list of dicts specifying the objects to select
    """

    def __init__(self, objects):
        sample_ids, object_ids = _parse_objects(objects)
        self._objects = objects
        self._sample_ids = sample_ids
        self._object_ids = object_ids
        self._pipeline = None

    @property
    def objects(self):
        """A list of dicts specifying the objects to select."""
        return self._objects

    def to_mongo(self, _, **__):
        if self._pipeline is None:
            raise ValueError(
                "`validate()` must be called before using a %s stage"
                % self.__class__
            )

        return self._pipeline

    def _kwargs(self):
        return [["objects", self._objects]]

    @classmethod
    def _params(self):
        return [
            {
                "name": "objects",
                "type": "dict",  # @todo use "list<dict>" when supported
                "placeholder": "[{...}]",
            }
        ]

    def _make_pipeline(self, sample_collection):
        label_schema = sample_collection.get_field_schema(
            ftype=fof.EmbeddedDocumentField, embedded_doc_type=fol.Label
        )

        pipeline = []

        stage = Select(self._sample_ids)
        stage.validate(sample_collection)
        pipeline.extend(stage.to_mongo(sample_collection))

        stage = SelectFields(list(self._object_ids.keys()))
        stage.validate(sample_collection)
        pipeline.extend(stage.to_mongo(sample_collection))

        for field, object_ids in self._object_ids.items():
            label_filter = expressions.ViewField("_id").is_in(
                [expressions.ObjectId(oid) for oid in object_ids]
            )
            stage = _make_label_filter_stage(label_schema, field, label_filter)
            if stage is None:
                continue

            stage.validate(sample_collection)
            pipeline.extend(stage.to_mongo(sample_collection))
        return pipeline

    def validate(self, sample_collection):
        self._pipeline = self._make_pipeline(sample_collection)


class SortBy(ViewStage):
    """Sorts the samples in a collection by the given field or expression.

    When sorting by an expression, ``field_or_expr`` can either be a
    :class:`fiftyone.core.expressions.ViewExpression` or a
    `MongoDB expression <https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#aggregation-expressions>`_
    that defines the quantity to sort by.

    Examples::

        import fiftyone as fo
        import fiftyone.zoo as foz
        from fiftyone import ViewField as F

        dataset = foz.load_zoo_dataset("quickstart")

        #
        # Sort the samples by their `uniqueness` field in ascending order
        #

        stage = fo.SortBy("uniqueness", reverse=False)
        view = dataset.add_stage(stage)

        #
        # Sorts the samples in descending order by the number of detections in
        # their `predictions` field whose bounding box area is less than 0.2
        #

        # Bboxes are in [top-left-x, top-left-y, width, height] format
        bbox = F("bounding_box")
        bbox_area = bbox[2] * bbox[3]

        small_boxes = F("predictions.detections").filter(bbox_area < 0.2)
        stage = fo.SortBy(small_boxes.length(), reverse=True)
        view = dataset.add_stage(stage)

    Args:
        field_or_expr: the field or expression to sort by
        reverse (False): whether to return the results in descending order
    """

    def __init__(self, field_or_expr, reverse=False):
        self._field_or_expr = field_or_expr
        self._reverse = reverse

    @property
    def field_or_expr(self):
        """The field or expression to sort by."""
        return self._field_or_expr

    @property
    def reverse(self):
        """Whether to return the results in descending order."""
        return self._reverse

    def to_mongo(self, _, **__):
        order = DESCENDING if self._reverse else ASCENDING

        field_or_expr = self._get_mongo_field_or_expr()

        if etau.is_str(field_or_expr):
            return [{"$sort": {field_or_expr: order}}]

        return [
            {"$set": {"_sort_field": field_or_expr}},
            {"$sort": {"_sort_field": order}},
            {"$unset": "_sort_field"},
        ]

    def _get_mongo_field_or_expr(self):
        if isinstance(self._field_or_expr, expressions.ViewField):
            return self._field_or_expr._expr

        if isinstance(self._field_or_expr, expressions.ViewExpression):
            return self._field_or_expr.to_mongo()

        return self._field_or_expr

    def _kwargs(self):
        return [
            ["field_or_expr", self._get_mongo_field_or_expr()],
            ["reverse", self._reverse],
        ]

    @classmethod
    def _params(cls):
        return [
            {"name": "field_or_expr", "type": "dict|str"},
            {
                "name": "reverse",
                "type": "bool",
                "default": "False",
                "placeholder": "reverse (default=False)",
            },
        ]

    def validate(self, sample_collection):
        field_or_expr = self._get_mongo_field_or_expr()

        # If sorting by a field, not an expression
        if etau.is_str(field_or_expr):
            # Make sure the field exists
            sample_collection.validate_fields_exist(field_or_expr)

            # Create an index on the field, if necessary, to make sorting
            # more efficient
            sample_collection.create_index(field_or_expr)


def _get_sample_ids(samples_or_ids):
    # avoid circular import...
    import fiftyone.core.collections as foc

    if etau.is_str(samples_or_ids):
        return [samples_or_ids]

    if isinstance(samples_or_ids, (fos.Sample, fos.SampleView)):
        return [samples_or_ids.id]

    if isinstance(samples_or_ids, foc.SampleCollection):
        return [s.id for s in samples_or_ids.select_fields()]

    if isinstance(next(iter(samples_or_ids)), (fos.Sample, fos.SampleView)):
        return [s.id for s in samples_or_ids]

    return list(samples_or_ids)


def _get_rng(seed):
    if seed is None:
        return random

    _random = random.Random()
    _random.seed(seed)
    return _random


def _get_labels_field(sample_collection, field_path):
    field, is_frame_field = _get_field(sample_collection, field_path)

    if isinstance(field, fof.EmbeddedDocumentField):
        document_type = field.document_type
        is_list_field = issubclass(document_type, fol._HasLabelList)
        if is_list_field:
            path = field_path + "." + document_type._LABEL_LIST_FIELD
        elif issubclass(document_type, fol._SINGLE_LABEL_FIELDS):
            path = field_path
        else:
            path = None

        if path is not None:
            return path, is_list_field, is_frame_field

    raise ValueError(
        "Field '%s' must be a Label type %s; found '%s'"
        % (field_path, fol._LABEL_FIELDS, field)
    )


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


def _parse_objects(objects):
    sample_ids = set()
    object_ids = defaultdict(set)
    for obj in objects:
        sample_ids.add(obj["sample_id"])
        object_ids[obj["field"]].add(obj["object_id"])

    return sample_ids, object_ids


def _make_label_filter_stage(label_schema, field, label_filter):
    if field not in label_schema:
        raise ValueError("Sample collection has no label field '%s'" % field)

    label_type = label_schema[field].document_type

    if label_type in fol._SINGLE_LABEL_FIELDS:
        return FilterField(field, label_filter)

    if label_type in fol._LABEL_LIST_FIELDS:
        return FilterLabels(field, label_filter)

    msg = "Ignoring unsupported field '%s' (%s)" % (field, label_type)
    warnings.warn(msg)
    return None


class _ViewStageRepr(reprlib.Repr):
    def repr_ViewExpression(self, expr, level):
        return self.repr1(expr.to_mongo(), level=level - 1)


_repr = _ViewStageRepr()
_repr.maxlevel = 2
_repr.maxdict = 3
_repr.maxlist = 3
_repr.maxtuple = 3
_repr.maxset = 3
_repr.maxstring = 30
_repr.maxother = 30


# Simple registry for the server to grab available view stages
_STAGES = [
    Exclude,
    ExcludeFields,
    ExcludeObjects,
    Exists,
    FilterField,
    FilterLabels,
    Limit,
    LimitLabels,
    MapLabels,
    Match,
    MatchTags,
    Mongo,
    Shuffle,
    Select,
    SelectFields,
    SelectObjects,
    SetField,
    Skip,
    SortBy,
    Take,
]
