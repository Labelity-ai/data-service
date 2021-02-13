from typing import List
from enum import Enum
from datumaro.components.dataset import Dataset, AnnotationType
from datumaro.components.extractor import Bbox, Polygon, Label, PolyLine, Points, DatasetItem
from app.models import ImageAnnotations


class DatasetExportFormat(Enum):
    CAMVID = 'camvid'
    PASCAL_VOC = 'voc'
    OBJECT_DETECTION_TFRECORD = 'tf_detection_api'
    COCO = 'coco'
    COCO_INSTANCES = 'coco_instances'
    COCO_LABELS = 'coco_labels'
    COCO_PERSON_KEYPOINTS = 'coco_person_keypoints'
    COCO_CAPTIONS = 'coco_captions'
    LABEL_ME = 'label_me'
    VOC_DETECTION = 'voc_detection'
    VOC_SEGMENTATION = 'voc_segmentation'
    VOC_CLASSIFICATION = 'voc_classification'
    CVAT_XML = 'cvat'


def create_datumaro_dataset(annotations: List[ImageAnnotations]):
    items = []

    for image_annotations in annotations:
        boxes = [Bbox(label=det.label,
                      attributes=det.attributes,
                      x=det.box[0],
                      y=det.box[1],
                      w=det.box[2] - det.box[0],
                      h=det.box[3] - det.box[1])
                 for det in image_annotations.detections]

        tags = [Label(label=tag.label, attributes=tag.attributes)
                for tag in image_annotations.tags]

        points = [Points(label=points.label,
                         points=points.points,
                         attributes=points.attributes)
                  for points in image_annotations.points]

        polygons = [Polygon(label=polygon.label,
                            points=polygon.points,
                            attributes=polygon.attributes)
                    for polygon in image_annotations.polygons]

        polylines = [PolyLine(label=polyline.label,
                              points=polyline.points,
                              attributes=polyline.attributes)
                     for polyline in image_annotations.polylines]

        item = DatasetItem(id=image_annotations.event_id,
                           annotations=boxes + polygons + polylines + points + tags)
        items.append(item)

    categories = []  # TODO
    dataset = Dataset.from_iterable(items, categories=categories)

    return dataset
