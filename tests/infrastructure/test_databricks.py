from pytest import fixture


@fixture
def resource(resource_management_client, request):
    resource_id = request.config.getoption("databricks_id")[0]
    return resource_management_client.resources.get_by_id(
        resource_id, api_version="2018-04-01"
    )


def test_sku_is_premium(resource):
    assert resource.sku.name == "premium"


def test_virtual_network_exists(resource):
    vnet_id = resource.properties.get("parameters", {}).get(
        "customVirtualNetworkId"
    )
    public_subnet_name = (
        resource.properties.get("parameters", {})
        .get("customPublicSubnetName", {})
        .get("value")
    )
    private_subnet_name = (
        resource.properties.get("parameters", {})
        .get("customPrivateSubnetName", {})
        .get("value")
    )
    assert vnet_id is not None
    assert public_subnet_name is not None
    assert private_subnet_name is not None


def test_location_allowed(resource, allowed_locations):
    assert resource.location in allowed_locations
