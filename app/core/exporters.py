from typing import List
from pathlib import Path
from enum import Enum
from datumaro.components.dataset import Dataset
from datumaro.components.extractor import Bbox, Polygon, Label, PolyLine, Points, DatasetItem, Caption
from datumaro.util.image import Image
from app.schema import ImageAnnotationsData


class DatasetExportFormat(str, Enum):
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


def create_datumaro_dataset(annotations: List[ImageAnnotationsData]):
    items = []
    labels = list(set(
        label.name
        for image_annotations in annotations
        for label in image_annotations.get_labels()
    ))
    labels_index = {v: i for i, v in enumerate(labels)}

    for image_annotations in annotations:
        boxes = [Bbox(label=labels_index[det.label],
                      attributes=det.attributes,
                      x=det.box[0],
                      y=det.box[1],
                      w=det.box[2] - det.box[0],
                      h=det.box[3] - det.box[1])
                 for det in image_annotations.detections]

        tags = [Label(label=tag.label, attributes=tag.attributes)
                for tag in image_annotations.tags]

        points = [Points(label=labels_index[points.label],
                         points=points.points,
                         attributes=points.attributes)
                  for points in image_annotations.points]

        polygons = [Polygon(label=labels_index[polygon.label],
                            points=polygon.points,
                            attributes=polygon.attributes)
                    for polygon in image_annotations.polygons]

        polylines = [PolyLine(label=labels_index[polyline.label],
                              points=polyline.points,
                              attributes=polyline.attributes)
                     for polyline in image_annotations.polylines]

        captions = [Caption(caption=caption) for caption in image_annotations.captions]

        image_path = image_annotations.event_id if image_annotations.has_image else None
        image = image_path and Image(path=image_path,
                                     size=(image_annotations.image_height, image_annotations.image_height))
        item = DatasetItem(id=Path(image_annotations.event_id).stem,
                           annotations=boxes + polygons + polylines + points + tags + captions,
                           image=image)
        items.append(item)

    dataset = Dataset.from_iterable(items, categories=labels)

    return dataset
