import sys
from types import ModuleType

for name, attributes in {
    "configuration": {"get_object_configuration_mapper": lambda: None},
    "logging_elody": {},
    "logging_elody.log": {
        "log": type("Log", (), {"debug": staticmethod(lambda *_, **__: None)})()
    },
    "serialization": {},
    "serialization.serialize": {"serialize": lambda *_, **__: None},
    "storage": {},
    "storage.storagemanager": {"StorageManager": object},
}.items():
    if name in sys.modules:
        continue
    module = ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    sys.modules[name] = module
