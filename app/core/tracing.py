from inspect import isclass, isroutine
from functools import wraps

from opencensus.trace.tracer import Tracer
from opencensus.trace.logging_exporter import LoggingExporter
from opencensus.trace.samplers import AlwaysOnSampler, ProbabilitySampler

from app.config import Config, TracingSampler

exporter = LoggingExporter()
sampler = AlwaysOnSampler() if Config.TRACING_SAMPLER == TracingSampler.ALWAYS else ProbabilitySampler()
tracer = Tracer(exporter=exporter, sampler=sampler)


def _traced(func, prefix=''):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with tracer.span(f'{prefix}.{func.__name__}'):
            return func(*args, **kwargs)
    return wrapper


def traced(*args):
    obj = args[0] if args else None

    if obj is None:
        return traced
    if isclass(obj):
        return _install_traceable_methods(obj)
    elif isroutine(obj):
        return _traced(obj)


def _is_special_name(name):
    return name.startswith("__") and name.endswith("__")


def _get_default_traceable_method_names(class_):
    default_traceable_method_names = []

    for (name, member) in class_.__dict__.items():
        if isroutine(member) and (not _is_special_name(name) or name in ("__init__", "__call__")):
            default_traceable_method_names.append(name)

    return default_traceable_method_names


def _install_traceable_methods(class_,):
    traceable_method_names = _get_default_traceable_method_names(class_)

    for method_name in traceable_method_names:
        descriptor = class_.__dict__[method_name]
        descriptor_type = type(descriptor)

        if descriptor_type is classmethod:
            tracing_proxy_descriptor = _make_traceable_classmethod(descriptor, prefix=class_.__name__)
        elif descriptor_type is staticmethod:
            tracing_proxy_descriptor = _make_traceable_staticmethod(descriptor, prefix=class_.__name__)
        else:
            tracing_proxy_descriptor = _traced(descriptor, class_.__name__)

        setattr(class_, method_name, tracing_proxy_descriptor)

    return class_


def _make_traceable_staticmethod(method_descriptor, **kwargs):
    return staticmethod(_traced(method_descriptor.__func__, **kwargs))


def _make_traceable_classmethod(method_descriptor, **kwargs):
    return classmethod(_traced(method_descriptor.__func__, **kwargs))
