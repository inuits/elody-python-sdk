"""A RabbitMQ message carries the trace from the service that sends it to the one that handles it.

The sender's span context goes into the message headers (W3C traceparent); the
consumer continues that trace, so an upload, the events it causes and the work
each consumer does form one trace in HyperDX. The message body is left alone:
its hash is the message id.
"""

import json

import pytest
from opentelemetry import baggage, context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode

from elody.observability import BaggageAttributes, signal_enabled, traced_rabbit


class FakeMessage:
    def __init__(self, body, headers, routing_key="dams.file_uploaded"):
        self.properties = {"headers": headers}
        self.method = {"routing_key": routing_key}
        self.message_id = "m-1"
        self._body = body

    def json(self):
        return self._body


class FakeAmqpstormRabbit:
    """Records what amqpstorm_flask's RabbitMQ would publish and register."""

    __module__ = "amqpstorm_flask.RabbitMQ"

    def __init__(self):
        self.published = []
        self.registered = []

    def send(self, body, routing_key, exchange_type="topic", **properties):
        self.published.append((body, routing_key, properties))

    def queue(self, routing_key, full_message_object=False, **kwargs):
        def decorator(f):
            self.registered.append((routing_key, full_message_object, kwargs, f))
            return f

        return decorator

    def deliver(self, body):
        """The registered consumer gets the message as published, headers and all."""
        published_body, routing_key, properties = self.published[-1]
        _, _, _, consumer = self.registered[-1]
        return consumer(FakeMessage(body, properties.get("headers", {}), routing_key))


@pytest.fixture
def spans():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_a_sent_message_carries_the_trace_in_its_headers(spans):
    provider, exporter = spans
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)
    body = {"specversion": "1.0", "data": {"id": "x"}}

    with provider.get_tracer("test").start_as_current_span("PUT /upload"):
        rabbit.send(body, routing_key="dams.file_uploaded", headers={"x-a": "1"})

    sent_body, routing_key, properties = rabbit.published[0]
    assert sent_body == body
    assert json.dumps(sent_body) == json.dumps(
        {"specversion": "1.0", "data": {"id": "x"}}
    )
    assert routing_key == "dams.file_uploaded"
    assert properties["headers"]["x-a"] == "1"
    assert properties["headers"]["traceparent"].startswith("00-")
    publish, upload = exporter.get_finished_spans()
    assert publish.kind == SpanKind.PRODUCER
    assert publish.name == "dams.file_uploaded publish"
    assert publish.parent.span_id == upload.context.span_id


def test_the_consumer_continues_the_senders_trace(spans):
    provider, exporter = spans
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)
    handled = []

    @rabbit.queue("dams.file_uploaded")
    def scan_file(routing_key, body, message_id):
        handled.append((routing_key, body, message_id))

    with provider.get_tracer("test").start_as_current_span("PUT /upload"):
        rabbit.send({"id": "x"}, routing_key="dams.file_uploaded")
    rabbit.deliver({"id": "x"})

    assert handled == [("dams.file_uploaded", {"id": "x"}, "m-1")]
    process = next(
        s for s in exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER
    )
    upload = next(s for s in exporter.get_finished_spans() if s.name == "PUT /upload")
    assert process.name == "dams.file_uploaded process"
    assert process.context.trace_id == upload.context.trace_id


def test_the_queue_keeps_its_name_and_settings(spans):
    provider, _ = spans
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)

    @rabbit.queue("dams.file_uploaded", auto_ack=False, prefetch_count=2)
    def scan_file(routing_key, body, message_id):
        pass

    routing_key, full_message_object, kwargs, consumer = rabbit.registered[0]
    assert consumer.__name__ == "scan_file"
    assert full_message_object is True
    assert kwargs == {"auto_ack": False, "prefetch_count": 2}


def test_a_consumer_that_wants_the_message_still_gets_it(spans):
    provider, _ = spans
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)
    received = []

    @rabbit.queue("dams.transcode", full_message_object=True)
    def transcode(message):
        received.append(message)

    rabbit.send({"id": "x"}, routing_key="dams.transcode")
    rabbit.deliver({"id": "x"})
    assert isinstance(received[0], FakeMessage)


def test_a_failing_consumer_ends_its_span_in_error_and_still_raises(spans):
    provider, exporter = spans
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)

    @rabbit.queue("dams.file_uploaded")
    def scan_file(routing_key, body, message_id):
        raise ValueError("clamd down")

    rabbit.send({"id": "x"}, routing_key="dams.file_uploaded")
    with pytest.raises(ValueError):
        rabbit.deliver({"id": "x"})
    process = next(
        s for s in exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER
    )
    assert process.status.status_code == StatusCode.ERROR
    assert [event.name for event in process.events] == ["exception"]


def test_another_amqp_library_only_gets_its_messages_traced(spans):
    provider, _ = spans

    class PikaRabbit(FakeAmqpstormRabbit):
        __module__ = "rabbitmq_pika_flask.RabbitMQ"

    rabbit = PikaRabbit()
    original_queue = rabbit.queue
    traced_rabbit(rabbit, provider)
    assert rabbit.queue == original_queue
    with provider.get_tracer("test").start_as_current_span("job"):
        rabbit.send({"id": "x"}, routing_key="dams.job_changed")
    assert "traceparent" in rabbit.published[0][2]["headers"]


def test_wrapping_twice_wraps_once(spans):
    provider, exporter = spans
    rabbit = traced_rabbit(traced_rabbit(FakeAmqpstormRabbit(), provider), provider)
    rabbit.send({"id": "x"}, routing_key="dams.file_uploaded")
    assert len(exporter.get_finished_spans()) == 1


def test_no_rabbit_is_no_rabbit():
    assert traced_rabbit(None) is None


@pytest.mark.parametrize(
    "signal, env, enabled",
    [
        ("METRICS", {}, False),
        (
            "METRICS",
            {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318"},
            True,
        ),
        (
            "METRICS",
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
                "OTEL_METRICS_EXPORTER": "none",
            },
            False,
        ),
        (
            "TRACES",
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
                "OTEL_SDK_DISABLED": "true",
            },
            False,
        ),
    ],
)
def test_each_signal_needs_a_collector_and_can_be_switched_off(signal, env, enabled):
    assert signal_enabled(signal, env) is enabled


# The pseudonymous user and library of the request (elody.* baggage, set by the
# GraphQL server) are on every span within it, and travel with a message.


def provider_with_baggage():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(BaggageAttributes())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def in_request_of(user, library, run):
    ctx = baggage.set_baggage("elody.user", user)
    ctx = baggage.set_baggage("elody.library", library, ctx)
    ctx = baggage.set_baggage("other", "left out", ctx)
    token = context.attach(ctx)
    try:
        return run()
    finally:
        context.detach(token)


def test_a_span_in_the_request_names_the_user_and_library():
    provider, exporter = provider_with_baggage()
    in_request_of(
        "p-1",
        "brugge.be",
        lambda: provider.get_tracer("t").start_span("GET /entities").end(),
    )
    (span,) = exporter.get_finished_spans()
    assert span.attributes["elody.user"] == "p-1"
    assert span.attributes["elody.library"] == "brugge.be"
    assert "other" not in span.attributes


def test_a_span_outside_a_request_names_nobody():
    provider, exporter = provider_with_baggage()
    provider.get_tracer("t").start_span("cron").end()
    (span,) = exporter.get_finished_spans()
    assert "elody.user" not in span.attributes


def test_the_consumer_of_a_message_knows_for_whom_it_works():
    provider, exporter = provider_with_baggage()
    rabbit = traced_rabbit(FakeAmqpstormRabbit(), provider)

    @rabbit.queue("dams.entity_changed")
    def index(routing_key, body, message_id):
        pass

    in_request_of(
        "p-1",
        "brugge.be",
        lambda: rabbit.send({"id": "x"}, routing_key="dams.entity_changed"),
    )
    rabbit.deliver({"id": "x"})
    consumer = next(
        s for s in exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER
    )
    assert consumer.attributes["elody.user"] == "p-1"
    assert consumer.attributes["elody.library"] == "brugge.be"


def test_the_browser_tabs_session_is_on_every_span_by_the_browsers_name():
    provider, exporter = provider_with_baggage()
    token = context.attach(baggage.set_baggage("elody.session", "s-1"))
    try:
        provider.get_tracer("t").start_span("GET /entities").end()
    finally:
        context.detach(token)
    (span,) = exporter.get_finished_spans()
    assert span.attributes["session_id"] == "s-1"
    assert "elody.session" not in span.attributes
