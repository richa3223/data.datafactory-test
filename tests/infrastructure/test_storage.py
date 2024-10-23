from pytest import fixture
from azure.mgmt.storage import StorageManagementClient


@fixture
def resource(storage_management_client: StorageManagementClient, request):
    resource_id = request.config.getoption("storage_id")[0].split("/")
    return storage_management_client.storage_accounts.get_properties(
        resource_id[4],
        resource_id[8]
    )


def test_encrypted_at_rest(resource):
    assert resource.encryption.services.blob.enabled


def test_encrypted_in_transit(resource):
    assert resource.enable_https_traffic_only


def test_minimum_tls(resource, allowed_tls_versions):
    assert resource.minimum_tls_version in allowed_tls_versions


def test_default_network_access(resource, allowed_network_access):
    assert resource.network_rule_set.default_action in allowed_network_access


def test_location_allowed(resource, allowed_locations):
    assert resource.location in allowed_locations


def test_hierarchical_namespace_not_enabled(resource):
    assert not resource.is_hns_enabled
