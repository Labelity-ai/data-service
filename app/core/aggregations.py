from app.models import ImageAnnotations, Label

GET_LABELS_PIPELINE = [
    # Select images within specific project
    # Select the labels field and set is as root of the pipeline
    {"$unwind": f"${+ImageAnnotations.labels}"},
    {"$replaceRoot": {"newRoot": f"${+ImageAnnotations.labels}"}},
    # Group labels by shape and name and create the field attributes as list of lists
    {'$group': {
        '_id': {'shape': f'${+Label.shape}', 'name': f'${+Label.name}'},
        'attributes': {
            '$push': f'${+Label.attributes}'
        }
    }},
    # Return name, shape, and flatten attributes without duplicates.
    {"$project": {
        "name": "$_id.name",
        "shape": "$_id.shape",
        "attributes": {
            "$reduce": {
                "input": "$attributes",
                "initialValue": [],
                "in": {"$setUnion": ["$$value", "$$this"]}
            }
        }
    }}
]

GET_IMAGE_ATTRIBUTES_PIPELINE = [
    {'$unwind': f'${+ImageAnnotations.attributes}'},
    {'$group': {
        '_id': '$_labels.attributes'
    }},
]
