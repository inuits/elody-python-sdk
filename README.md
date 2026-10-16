# elody SDK for Python

## Installation

To install the Python SDK library using pip:
```
pip install elody
```

## Usage

Begin by importing the `elody` module:
```
import elody
```

Then construct a client object with the url to the elody collection service and
JWT-token:
```
client = elody.Client(elody_collection_url=collection_url, static_jwt=jwt_token)
```

For production, you can specify the `ELODY_COLLECTION_URL` and `STATIC_JWT`
environment variables instead of specifying the key and secret explicitly.

## Examples

### Creating an object

```
object = {
    "identifiers": ["test"],
    "type": "asset",
    "metadata": [
        {
            "key": "title",
            "value": "test",
            "lang": "en",
        }
    ]
}

client.add_object("entities", object)
```

### Getting an object

```
object = client.get_object("entities", "test")
print(object)
```

### Updating an object

```
object_update = {
    "identifiers": ["test"],
    "type": "asset",
    "metadata": [
        {
            "key": "title",
            "value": "test UPDATE",
            "lang": "en",
        }
    ]
}

client.update_object("entities", "test", object_update)
```

### Deleting an object

```
client.delete_object("entities", "test")
```

## Observability

`pip install elody[observability]` adds `elody.observability`: traces, metrics,
logs and errors to an OpenTelemetry collector (HyperDX), for every Elody Flask
service the same way.

```python
from elody.observability import init_observability, record_exception, traced_rabbit

app = Flask(__name__)
init_observability(app)  # Flask and outgoing requests; metrics; the root logger's records
rabbit = traced_rabbit(amqpstorm_flask.RabbitMQ(...))  # before its queues are registered
```

Nothing is sent unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set and `OTEL_SDK_DISABLED`
is not `true`; `OTEL_SERVICE_NAME` names the service, `OTEL_RESOURCE_ATTRIBUTES`
adds e.g. the client (`service.namespace`). A RabbitMQ message carries the
trace (W3C traceparent) and the request's `elody.*` baggage (the pseudonymous
user, library and browser session, set by the GraphQL server) to its consumer,
whose spans name them. `record_exception(error)` records an unexpected error
(not a 4xx, not a missing entity) on the current span.

Tests: `pip install -e .[observability] pytest && pytest tests`.
