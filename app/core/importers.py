from enum import Enum
from datumaro.components.dataset import Dataset, DatasetItem, AnnotationType
from pathlib import Path
from app.models import ImageAnnotations, Tag, Polygon, Detection, Polyline, Keypoints


class DatasetImportFormat(Enum):
    CAMVID = 'camvid'
    COCO = 'coco'
    CVAT_XML = 'cvat'
    DATUMARO = 'datumaro'
    IMAGENET = 'imagenet'
    LABEL_ME = 'label_me'
    OBJECT_DETECTION_TFRECORD = 'tf_detection_api'
    VOC = 'voc'
    YOLO = 'yolo'


def _normalize_points(points, item: DatasetItem):
    if not item.has_image:
        return points

    result = points[:]
    width, height = item.image.size

    for i in range(len(points) - 1):
        result[i] /= width if i % 2 == 0 else height

    return result


def import_dataset(input_file: Path, format: DatasetImportFormat):
    dataset = Dataset.import_from(input_file, format=format)
    labels = dataset.categories()
    annotations = []

    for row in dataset:
        item: DatasetItem = row
        event_id = item.image.path.split('/')[-1] if item.has_image else item.id
        tags, detections, polygons, polylines, points = [], [], [], [], []

        for annotation in item.annotations:
            label = labels[annotation.type][annotation.label]

            if annotation.type == AnnotationType.label:
                tags.append(
                    Tag(label=label.name, attributes=annotation.attributes)
                )

            if annotation.type == AnnotationType.bbox:
                box = _normalize_points(annotation.get_bbox(), item)
                detections.append(
                    Detection(label=label.name, attributes=annotation.attributes, box=box)
                )

            if annotation.type == AnnotationType.polygon:
                points = _normalize_points(annotation.points, item)
                polygons.append(
                    Polygon(label=label.name, attributes=annotation.attributes, points=points)
                )

            if annotation.type == AnnotationType.polyline:
                points = _normalize_points(annotation.points, item)
                polylines.append(
                    Polyline(label=label.name, attributes=annotation.attributes, points=points)
                )

            if annotation.type == AnnotationType.points:
                points = _normalize_points(annotation.points, item)
                points.append(
                    Keypoints(label=label.name, attributes=annotation.attributes, points=points)
                )

        image_annotations = ImageAnnotations(
            attributes=item.attributes,
            event_id=event_id,
            tags=tags,
            polygons=polygons,
            detections=detections,
            polylines=polylines,
            points=points,
        )

        annotations.append(image_annotations)

    return annotations
