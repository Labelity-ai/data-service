import operator

from app.models import QueryExpression
from app.core.query_engine.expressions import ViewExpression, ViewField


OPERATORS = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': operator.truediv,
    '%': operator.mod,
    '^': operator.xor,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt,
    '<=': operator.le,
    '<': operator.lt,
    '&&': operator.and_,
    '||': operator.or_,
    '~': operator,
}

METHOD_OPERATORS = {
    'abs': 'abs',
    'floor': 'floor',
    'ceil': 'ceil',
    'round': 'round',
    'truncate': 'trunc',
    'exp': 'exp',
    'ln': 'ln',
    'log': 'log',
    'log10': 'log10',
    'pow': 'pow',
    'sqrt': 'sqrt',
    'is_null': 'is_null',
    'is_number': 'is_number',
    'is_string': 'is_array',
    'is_missing': 'is_missing',
    'in': 'is_in',
    'apply': 'apply',
    'if_else': 'if_else',
    'cases': 'cases',
    'switch': 'switch',
    'map_values': 'map_values',
    'set_field': 'set_field',
    'let_in': 'let_in',
    'min': 'min',
    'max': 'max',
    'length': 'length',
    'contains': 'contains',
    'reverse': 'reverse',
    'sort': 'sort',
    'filter': 'filter',
    'map': 'map',
    'prepend': 'prepend',
    'append': 'append',
    'insert': 'insert',
    'extend': 'extend',
    'sum': 'sum',
    'mean': 'mean',
    'std': 'std',
    'reduce': 'reduce',
    'join': 'join',
    'substr': 'substr',
    'std': 'std',
    'strlen': 'strlen',
    'lower': 'lower',
    'upper': 'upper',
    'concat': 'concat',
    'strip': 'strip',
    'lstrip': 'lstrip',
    'rstrip': 'rstrip',
    'replace': 'replace',
    're_match': 're_match',
    'starts_with': 'starts_with',
    'ends_with': 'ends_with',
    'contains_str': 'contains_str',
    'matches_str': 'matches_str',
    'split': 'split',
    'rsplit': 'rsplit',
    'literal': 'literal',
    'rand': 'rand',
    'range': 'range',
    'enumerate': 'enumerate',
    'zip': 'zip',
    'index': '__getitem___'
}


def construct_view_expression(query: QueryExpression) -> ViewExpression:
    parameters = {}

    for k, v in query.parameters:
        if isinstance(k, QueryExpression):
            parameters[k] = construct_view_expression(k)
        else:
            parameters[k] = v

    if query.field == '_':
        query.field = None

    expression = ViewExpression(query.literal) if query.literal else ViewField(query.field)

    if not query.operator:
        return expression

    if query.operator in OPERATORS:
        return OPERATORS[query.operator](expression, **parameters)
    elif query.operator in METHOD_OPERATORS:
        return expression.__getattribute__(query.operator)(**parameters)
    else:
        # TODO: Throw error
        pass
