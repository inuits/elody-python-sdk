import pytest

from elody.policies.permission_handler import restrict_keys_per_element


class Tenant:
    roles = ["venue_member"]


class UserContext:
    def __init__(self):
        self.x_tenant = Tenant()
        self.bag = {"restricted_keys": []}


def user(*organizations):
    return {
        "type": "user",
        "properties": {
            "email": [{"value": "a@b.c"}],
            "ref_organizations": [
                {"value": organization, "roles": ["admin"], "function": ["technical"]}
                for organization in organizations
            ],
        },
    }


def organizations_of(item):
    return [
        (organization["value"], organization.get("roles"))
        for organization in item["properties"]["ref_organizations"]
    ]


def restrict(item, restricted_key, conditions, crud="read"):
    from elody.util import flatten_dict

    restrict_keys_per_element(
        UserContext(), item, flatten_dict({}, item), restricted_key, conditions, crud, {}, {}
    )
    return item


def test_only_elements_matching_the_condition_lose_the_key():
    item = restrict(
        user("ORG-1", "ORG-2"),
        "properties.ref_organizations[].roles",
        {"!properties.ref_organizations[].value": ["ORG-2"]},
    )

    assert organizations_of(item) == [("ORG-1", None), ("ORG-2", ["admin"])]


def test_sibling_keys_of_a_stripped_element_survive():
    item = restrict(
        user("ORG-1"),
        "properties.ref_organizations[].roles",
        {"!properties.ref_organizations[].value": ["ORG-2"]},
    )

    element = item["properties"]["ref_organizations"][0]
    assert element["value"] == "ORG-1"
    assert element["function"] == ["technical"]


def test_a_condition_matching_no_element_strips_nothing():
    item = restrict(
        user("ORG-1", "ORG-2"),
        "properties.ref_organizations[].roles",
        {"properties.ref_organizations[].value": ["ORG-9"]},
    )

    assert organizations_of(item) == [("ORG-1", ["admin"]), ("ORG-2", ["admin"])]


def test_an_unconditional_restriction_strips_every_element():
    item = restrict(user("ORG-1", "ORG-2"), "properties.ref_organizations[].roles", {})

    assert organizations_of(item) == [("ORG-1", None), ("ORG-2", None)]


def test_a_document_without_the_list_is_left_alone():
    item = {"type": "user", "properties": {"email": [{"value": "a@b.c"}]}}

    restrict(item, "properties.ref_organizations[].roles", {"!x[].y": ["ORG-2"]})

    assert item == {"type": "user", "properties": {"email": [{"value": "a@b.c"}]}}


def test_an_element_the_condition_cannot_be_evaluated_on_is_restricted():
    item = {
        "type": "user",
        "properties": {"ref_organizations": [{"roles": ["admin"]}]},
    }

    restrict(
        item,
        "properties.ref_organizations[].roles",
        {"!properties.ref_organizations[].value": ["ORG-2"]},
    )

    assert "roles" not in item["properties"]["ref_organizations"][0]


def test_a_condition_without_the_element_marker_is_scoped_to_the_document():
    item = user("ORG-1", "ORG-2")
    item["properties"]["enabled"] = [{"value": True}]

    restrict(
        item,
        "properties.ref_organizations[].roles",
        {"properties.enabled.value": [False]},
    )

    assert organizations_of(item) == [("ORG-1", ["admin"]), ("ORG-2", ["admin"])]


def test_a_write_restriction_on_an_element_key_is_refused_loudly():
    with pytest.raises(Exception, match="not supported"):
        restrict(
            user("ORG-1"),
            "properties.ref_organizations[].roles",
            {},
            crud="update",
        )


def restricted_keys(item, restricted_key, conditions, key_to_check):
    from elody.util import flatten_dict

    user_context = UserContext()
    restrict_keys_per_element(
        user_context,
        item,
        flatten_dict({}, item),
        restricted_key,
        conditions,
        "read",
        {},
        {},
        key_to_check=key_to_check,
    )
    return user_context.bag["restricted_keys"]


KEY = "properties.ref_organizations[].roles"
NOT_MINE = {"!properties.ref_organizations[].value": ["ORG-2"]}


def test_key_to_check_reports_nothing_while_one_element_keeps_the_key():
    assert restricted_keys(user("ORG-1", "ORG-2"), KEY, NOT_MINE, KEY) == []


def test_key_to_check_reports_the_key_when_no_element_keeps_it():
    assert restricted_keys(user("ORG-1"), KEY, NOT_MINE, KEY) == [KEY]


def test_key_to_check_reports_the_key_when_the_list_is_empty():
    assert restricted_keys(user(), KEY, NOT_MINE, KEY) == [KEY]


def test_a_different_key_to_check_is_never_reported():
    assert restricted_keys(user("ORG-1"), KEY, NOT_MINE, "last_seen_time") == []
