"""Errors and logs go to the OpenTelemetry collector (HyperDX), not to Sentry.

An unexpected error is recorded on the request's span, so it shows in HyperDX
next to the trace it broke; client errors (4xx, not found) are not, as Sentry's
filter left them out before. Logs are exported over OTLP only when OpenTelemetry
is switched on, and carry the trace they were written in.
"""

import logging

import pytest
from elody.exceptions import NotFoundException
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode
from werkzeug.exceptions import BadRequest, Forbidden, InternalServerError, NotFound

from elody.observability import is_reportable, log_export_enabled, record_exception


@pytest.mark.parametrize(
    "exception, reportable",
    [
        (ValueError("boom"), True),
        (InternalServerError(), True),
        (BadRequest(), False),
        (Forbidden(), False),
        (NotFound(), False),
        (NotFoundException("gone"), False),
    ],
)
def test_only_unexpected_errors_are_reported(exception, reportable):
    assert is_reportable(exception) is reportable


def test_an_unexpected_error_is_recorded_on_the_current_span():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("GET /entities"):
        record_exception(ValueError("boom"))
    with tracer.start_as_current_span("GET /entities/x"):
        record_exception(NotFound())

    broken, missing = exporter.get_finished_spans()
    assert broken.status.status_code == StatusCode.ERROR
    assert [event.name for event in broken.events] == ["exception"]
    assert broken.events[0].attributes["exception.type"] == "ValueError"
    assert missing.status.status_code == StatusCode.UNSET
    assert missing.events == ()


def test_recording_without_a_span_does_nothing():
    assert trace.get_current_span() is trace.INVALID_SPAN
    record_exception(ValueError("boom"))  # no error


@pytest.mark.parametrize(
    "env, enabled",
    [
        ({}, False),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318"}, True),
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
                "OTEL_SDK_DISABLED": "true",
            },
            False,
        ),
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
                "OTEL_LOGS_EXPORTER": "none",
            },
            False,
        ),
    ],
)
def test_logs_are_exported_only_when_opentelemetry_is_on(env, enabled):
    assert log_export_enabled(env) is enabled


def test_the_log_handler_is_left_off_when_disabled(monkeypatch):
    from elody.observability import init_log_export

    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    before = list(logging.getLogger().handlers)
    assert init_log_export() is False
    assert logging.getLogger().handlers == before
