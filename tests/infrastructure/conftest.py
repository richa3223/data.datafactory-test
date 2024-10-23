from multiprocessing.sharedctypes import Value
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.cli.core import AzCli
from azure.cli.core._profile import Profile
from pytest import fixture

import os


@fixture(scope="session")
def resource_management_client():
    return _get_client(ResourceManagementClient)


@fixture(scope="session")
def storage_management_client():
    return _get_client(StorageManagementClient)


def _get_client(client_type):
    cli_ctx = AzCli(
        config_dir=os.path.expanduser(
            os.environ.get("AZURE_CONFIG_DIR", "~/.azure")
        )
    )
    profile = Profile(cli_ctx=cli_ctx)
    cred, subscription_id, tenant_id = profile.get_login_credentials(
        resource=cli_ctx.cloud.endpoints.resource_manager
    )
    return client_type(
        credential=cred,
        subscription_id=subscription_id,
        tenant_id=tenant_id
    )


@fixture(scope="session")
def allowed_locations(request):
    return split_option(request, "allowed_locations")


@fixture(scope="session")
def allowed_tls_versions(request):
    return split_option(request, "allowed_tls_versions")


@fixture(scope="session")
def allowed_network_access(request):
    return split_option(request, "allowed_network_access")


def split_option(request, name, default=[]):
    option = request.config.getoption(name)
    if option is None:
        return default
    return [None if x == "None" else x for x in [x.strip() for x in option[0].split(",")]]


def pytest_addoption(parser):
    try:
        parser.addoption(
            "--allowed-locations",
            nargs=1,
            action="store",
            type=str,
            help="Allowed locations for resources",
            default=[os.environ.get(
                "ALLOWED_LOCATIONS",
                "uksouth,ukwest,UK South,UK West"
            )]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--allowed-tls-versions",
            nargs=1,
            action="store",
            type=str,
            help="Allowed locations for resources",
            default=[os.environ.get(
                "ALLOWED_TLS_VERSIONS",
                "TLS1_2"
            )]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--allowed-network-access",
            nargs=1,
            action="store",
            type=str,
            help="Allowed locations for resources",
            default=[os.environ.get(
                "ALLOWED_NETWORK_ACCESS",
                "Deny"
            )]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--databricks-id",
            nargs=1,
            action="store",
            type=str,
            help="Databricks resource ID",
            default=[os.environ.get("DATABRICKS_ID")]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--datafactory-id",
            nargs=1,
            action="store",
            type=str,
            help="Data factory resource ID",
            default=[os.environ.get("DATAFACTORY_ID")]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--datalake-id",
            nargs=1,
            action="store",
            type=str,
            help="Data lake resource ID",
            default=[os.environ.get("DATALAKE_ID")]
        )
    except ValueError:
        pass
    try:
        parser.addoption(
            "--storage-id",
            nargs=1,
            action="store",
            type=str,
            help="Storage account resource ID",
            default=[os.environ.get("STORAGE_ID")]
        )
    except ValueError:
        pass
