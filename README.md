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
