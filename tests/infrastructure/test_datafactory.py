from pytest import fixture


@fixture
def resource(resource_management_client, request):
    resource_id = request.config.getoption("datafactory_id")[0]
    return resource_management_client.resources.get_by_id(
        resource_id, api_version="2018-06-01"
    )


def test_system_assigned_identity(resource):
    assert resource.identity.type == "SystemAssigned"


def test_location_allowed(resource, allowed_locations):
    assert resource.location in allowed_locations
