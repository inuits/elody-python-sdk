"""Traces, metrics, logs and errors to the OpenTelemetry collector (HyperDX), in place of Sentry.

For every Elody Python service: pip install elody[observability], then
init_observability(app) next to the Flask app and traced_rabbit(...) around the
AMQP client, before its queues are registered. Nothing is sent unless a collector
is configured (OTEL_EXPORTER_OTLP_ENDPOINT) and OpenTelemetry is not switched off
(OTEL_SDK_DISABLED; a single signal with OTEL_<SIGNAL>_EXPORTER=none). The
service is named by OTEL_SERVICE_NAME.

- traces: incoming Flask requests, outgoing `requests` calls, and RabbitMQ: a
  message carries the trace in its headers (W3C traceparent) from the service
  that sends it to the one that handles it;
- metrics: the durations of those requests, in and out;
- logs: the root logger's records, each with its trace and span;
- errors: an unexpected one is recorded on the request's span, so it is found
  next to the trace it broke; client errors are left out, as Sentry's filter did;
- who: the pseudonymous user and library of the request and the browser tab's
  session (elody.* baggage, set by the GraphQL server) are copied onto every
  span, also across RabbitMQ.
"""

import logging
from functools import wraps
from os import environ
from typing import Any, Mapping

from opentelemetry import baggage, context, propagate, trace
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode
from werkzeug.exceptions import HTTPException, NotFound

from elody.exceptions import NotFoundException

_NOT_REPORTED = (NotFoundException, NotFound)


def is_reportable(exception: BaseException) -> bool:
    """An unexpected error, not a client's: not a 4xx, not a missing entity."""
    if isinstance(exception, _NOT_REPORTED):
        return False
    if isinstance(exception, HTTPException):
        return (exception.code or 500) >= 500
    return True


def _record_on(span, exception: BaseException) -> None:
    span.record_exception(exception)
    span.set_status(
        Status(StatusCode.ERROR, f"{exception.__class__.__name__}: {exception}")
    )


def record_exception(exception: BaseException) -> None:
    """Records a reportable error on the current span, which then ends in error."""
    if not is_reportable(exception):
        return
    span = trace.get_current_span()
    if span.is_recording():
        _record_on(span, exception)


def signal_enabled(signal: str, env: Mapping[str, str] = environ) -> bool:
    """TRACES, METRICS or LOGS go out when a collector is configured and nothing switches them off."""
    return (
        bool(env.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
        and env.get("OTEL_SDK_DISABLED", "false").lower() != "true"
        and env.get(f"OTEL_{signal}_EXPORTER", "otlp").lower() != "none"
    )


def log_export_enabled(env: Mapping[str, str] = environ) -> bool:
    return signal_enabled("LOGS", env)


def _resource():
    from opentelemetry.sdk.resources import Resource

    return Resource.create()


def init_log_export(level: int = logging.INFO) -> bool:
    """Sends the root logger's records to the collector; returns whether it does."""
    if not log_export_enabled():
        return False
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    provider = LoggerProvider(resource=_resource())
    provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(provider)
    logging.getLogger().addHandler(
        LoggingHandler(level=level, logger_provider=provider)
    )
    return True


# a baggage entry that goes on the spans by another name: the browser tab's
# session, named as the browser's own spans name it, so one search finds all
_ATTRIBUTE_FOR_BAGGAGE = {"elody.session": "session_id"}


class BaggageAttributes(SpanProcessor):
    """Copies the elody.* baggage entries (user pseudonym, library, tab session) onto each span started."""

    def on_start(self, span, parent_context=None) -> None:
        for key, value in baggage.get_all(parent_context).items():
            if key.startswith("elody.") and isinstance(value, str):
                span.set_attribute(_ATTRIBUTE_FOR_BAGGAGE.get(key, key), value)


def _add_baggage_attributes(provider) -> None:
    if not getattr(provider, "_elody_baggage_attributes", False):
        provider.add_span_processor(BaggageAttributes())
        provider._elody_baggage_attributes = True


def _init_traces() -> None:
    from opentelemetry.sdk.trace import TracerProvider

    # a service that sets up its own provider (collection-api's tracing.py) keeps it
    if isinstance(trace.get_tracer_provider(), TracerProvider):
        _add_baggage_attributes(trace.get_tracer_provider())
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=_resource())
    _add_baggage_attributes(provider)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def _init_metrics() -> None:
    from opentelemetry import metrics
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    metrics.set_meter_provider(
        MeterProvider(resource=_resource(), metric_readers=[reader])
    )


def init_observability(app: Any = None) -> bool:
    """Switches on what the environment asks for and instruments the app; returns whether anything is on."""
    if not any(signal_enabled(signal) for signal in ("TRACES", "METRICS", "LOGS")):
        return False
    if signal_enabled("TRACES"):
        _init_traces()
    if signal_enabled("METRICS"):
        _init_metrics()
    init_log_export()
    from opentelemetry.instrumentation.flask import FlaskInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor

    if app is not None and not getattr(app, "_is_instrumented_by_opentelemetry", False):
        FlaskInstrumentor().instrument_app(app)
    if not RequestsInstrumentor().is_instrumented_by_opentelemetry:
        RequestsInstrumentor().instrument()
    return True


def _headers_of(message: Any) -> dict:
    properties = getattr(message, "properties", None) or {}
    headers = properties.get("headers") or {}
    return {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in headers.items()
    }


def _messaging_attributes(routing_key: Any) -> dict:
    return {
        "messaging.system": "rabbitmq",
        "messaging.rabbitmq.destination.routing_key": str(routing_key),
    }


def traced_rabbit(rabbit: Any, tracer_provider: Any = None) -> Any:
    """The rabbit client, its messages carrying the trace from sender to consumer.

    Every send gets a producer span and the trace context in its headers; the body
    is left as it is (amqpstorm_flask hashes it into the message id). With
    amqpstorm_flask, which hands a consumer only the body, each consumer is
    registered for the whole message instead: the wrapper reads the headers,
    continues the trace in a consumer span, then calls the consumer as the
    library would have. Other AMQP libraries get their sends traced only.
    """
    if rabbit is None or getattr(rabbit, "_elody_traced", False):
        return rabbit

    def tracer():
        return (tracer_provider or trace.get_tracer_provider()).get_tracer(__name__)

    send = rabbit.send

    @wraps(send)
    def traced_send(body, routing_key, *args, **properties):
        with tracer().start_as_current_span(
            f"{routing_key} publish",
            kind=SpanKind.PRODUCER,
            attributes=_messaging_attributes(routing_key),
        ):
            headers = dict(properties.get("headers") or {})
            propagate.inject(headers)
            return send(body, routing_key, *args, **{**properties, "headers": headers})

    rabbit.send = traced_send
    rabbit._elody_traced = True
    if not type(rabbit).__module__.startswith("amqpstorm_flask"):
        return rabbit

    queue = rabbit.queue

    @wraps(queue)
    def traced_queue(routing_key, *args, full_message_object=False, **kwargs):
        def decorator(consumer):
            @wraps(consumer)
            def consume(message):
                key = (getattr(message, "method", None) or {}).get(
                    "routing_key"
                ) or routing_key
                token = context.attach(propagate.extract(_headers_of(message)))
                try:
                    with tracer().start_as_current_span(
                        f"{key} process",
                        kind=SpanKind.CONSUMER,
                        attributes=_messaging_attributes(key),
                        record_exception=False,
                        set_status_on_exception=False,
                    ) as span:
                        try:
                            if full_message_object:
                                return consumer(message)
                            return consumer(
                                routing_key=routing_key,
                                body=message.json(),
                                message_id=message.message_id,
                            )
                        except Exception as exception:
                            _record_on(span, exception)
                            raise
                finally:
                    context.detach(token)

            return queue(routing_key, *args, full_message_object=True, **kwargs)(
                consume
            )

        return decorator

    rabbit.queue = traced_queue
    return rabbit
