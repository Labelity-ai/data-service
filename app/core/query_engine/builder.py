from typing import List, Dict
from app.schema import AnnotationsQuery, QueryRule
from app.models import Shape


OPERATOR_TO_MONGO = {

}

SHAPE_TO_FIELD = {
    Shape.TAG: 'tags',
    Shape.POINT: 'points',
    Shape.BOX: 'detections',
    Shape.POLYGON: 'polygons',
    Shape.POLYLINE: 'polylines',
}


def _build_condition_query(rule: QueryRule) -> Dict:
    pass


def construct_mongo_query(query: AnnotationsQuery) -> List[Dict]:
    rules = []

    for rule in query.rules:
        if isinstance(rule, QueryRule):
            field = SHAPE_TO_FIELD[rule.field_shape]

            q = {
                field: {
                    '$filter': {
                        'input': f'${field}',
                        'as': field,
                        'cond': _build_condition_query(rule)
                    }
                }
            }

            pass

    return []
