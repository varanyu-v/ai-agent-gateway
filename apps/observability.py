import logging
import os
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import metrics, propagate, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_NAMESPACE, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Link, SpanKind, Tracer


TRACE_CONTEXT_FIELD = "trace_context"
_CONFIGURED_SERVICES: set[str] = set()
_HTTPX_INSTRUMENTED = False
_ASYNCPG_INSTRUMENTED = False
_LOGGING_INSTRUMENTED = False


def _otel_enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "true").lower() not in {"0", "false", "no", "off"}


def _traces_endpoint() -> str | None:
    explicit_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    if explicit_endpoint:
        return explicit_endpoint

    base_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not base_endpoint:
        return None

    return f"{base_endpoint.rstrip('/')}/v1/traces"


def _metrics_endpoint() -> str | None:
    explicit_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
    if explicit_endpoint:
        return explicit_endpoint

    base_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not base_endpoint:
        return None

    return f"{base_endpoint.rstrip('/')}/v1/metrics"


def _metric_export_interval_ms() -> int:
    try:
        return int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "10000"))
    except ValueError:
        return 10000


def _resource(service_name: str) -> Resource:
    return Resource.create(
        {
            SERVICE_NAME: service_name,
            SERVICE_NAMESPACE: os.getenv(
                "OTEL_SERVICE_NAMESPACE",
                "ai-agent-gateway",
            ),
            "deployment.environment": os.getenv("DEPLOYMENT_ENVIRONMENT", "local"),
        },
    )


def setup_observability(service_name: str, app: Any | None = None) -> Tracer:
    global _ASYNCPG_INSTRUMENTED, _HTTPX_INSTRUMENTED, _LOGGING_INSTRUMENTED

    if not _otel_enabled():
        return trace.get_tracer(service_name)

    if service_name not in _CONFIGURED_SERVICES:
        resource = _resource(service_name)
        provider = TracerProvider(resource=resource)
        endpoint = _traces_endpoint()
        if endpoint:
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)),
            )
        trace.set_tracer_provider(provider)

        metrics_endpoint = _metrics_endpoint()
        if metrics_endpoint:
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=metrics_endpoint),
                export_interval_millis=_metric_export_interval_ms(),
            )
            metrics.set_meter_provider(
                MeterProvider(resource=resource, metric_readers=[metric_reader]),
            )
        _CONFIGURED_SERVICES.add(service_name)

    provider = trace.get_tracer_provider()
    meter_provider = metrics.get_meter_provider()

    if app is not None:
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=provider,
            meter_provider=meter_provider,
        )

    if not _HTTPX_INSTRUMENTED:
        HTTPXClientInstrumentor().instrument(tracer_provider=provider)
        _HTTPX_INSTRUMENTED = True

    if not _ASYNCPG_INSTRUMENTED:
        AsyncPGInstrumentor().instrument(tracer_provider=provider)
        _ASYNCPG_INSTRUMENTED = True

    if not _LOGGING_INSTRUMENTED:
        LoggingInstrumentor().instrument(set_logging_format=True)
        logging.getLogger(__name__).info(
            "OpenTelemetry configured for service=%s endpoint=%s",
            service_name,
            _traces_endpoint() or "none",
        )
        _LOGGING_INSTRUMENTED = True

    return trace.get_tracer(service_name)


def clean_attributes(attributes: Mapping[str, Any] | None) -> dict[str, Any]:
    if not attributes:
        return {}

    clean: dict[str, Any] = {}
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, set):
            clean[key] = sorted(str(item) for item in value)
        elif isinstance(value, (list, tuple)):
            clean[key] = [
                item
                for item in value
                if isinstance(item, (bool, int, float, str))
            ]
        elif isinstance(value, (bool, int, float, str)):
            clean[key] = value
        else:
            clean[key] = str(value)
    return clean


def trace_headers() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def inject_trace_context(payload: dict[str, Any]) -> dict[str, Any]:
    carrier = trace_headers()
    if not carrier:
        return payload
    return {**payload, TRACE_CONTEXT_FIELD: carrier}


def extract_trace_context(event: Mapping[str, Any]) -> Any:
    trace_context = event.get(TRACE_CONTEXT_FIELD)
    if not isinstance(trace_context, Mapping):
        trace_context = {}
    return propagate.extract(dict(trace_context))


def trace_link_from_event(event: Mapping[str, Any]) -> Link | None:
    context = extract_trace_context(event)
    span_context = trace.get_current_span(context).get_span_context()
    if not span_context.is_valid:
        return None
    return Link(span_context)


@contextmanager
def start_event_span(
    tracer: Tracer,
    name: str,
    event: Mapping[str, Any],
    *,
    attributes: Mapping[str, Any] | None = None,
    kind: SpanKind = SpanKind.CONSUMER,
):
    context = extract_trace_context(event)
    with tracer.start_as_current_span(
        name,
        context=context,
        kind=kind,
        attributes=clean_attributes(attributes),
    ) as span:
        yield span
