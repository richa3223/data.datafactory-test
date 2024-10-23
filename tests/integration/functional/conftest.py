from azure.storage.blob import BlobServiceClient, ContainerClient, generate_blob_sas
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import GenericResource
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.multiapi.storagev2.filedatalake.v2020_02_10 import (
    DataLakeDirectoryClient,
    DataLakeServiceClient,
    FileSystemClient
)
from azure.core.exceptions import ResourceExistsError
from azure.cli.core import AzCli
from azure.cli.core._profile import Profile
from pyspark.sql import SparkSession
from pytest import fixture
from typing import Callable, Any
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi
from databricks import sql
from databricks.sql.client import Connection

import os
import uuid
import datetime
import time


@fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-azure:3.3.1",
        )
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    yield spark


@fixture(scope="session")
def unique_identifier() -> str:
    return str(uuid.uuid4()).replace("-", "")


@fixture(scope="session")
def location(unique_identifier: str) -> str:
    location = f"test-{unique_identifier}"
    return location


@fixture(scope="session")
def storage_account_key(
    storage_management_client: StorageManagementClient,
    request
) -> Callable[[], str]:
    """
    """
    resource_id = request.config.getoption("storage_id")[0].split("/")

    def _storage_account_key():
        keys = storage_management_client.storage_accounts.list_keys(
            resource_id[4],
            resource_id[8]
        )
        return keys.keys[0].value
        
    return _storage_account_key


@fixture(scope="session")
def blob_container(
    storage_account_key: Callable[[], str],
    location: str,
    request
) -> ContainerClient:
    """
    """
    resource_id = request.config.getoption("storage_id")[0].split("/")
    client = BlobServiceClient(
        f"https://{resource_id[8]}.blob.core.windows.net",
        credential=storage_account_key()
    )
    container_client = client.create_container(location)
    yield container_client
    container_client.delete_container()


@fixture(scope="session")
def generate_sas(
    blob_container: ContainerClient,
    storage_account_key: Callable[[], str]
) -> Callable[[str], str]:
    return lambda blob_name: generate_blob_sas(
        account_name=blob_container.account_name,
        container_name=blob_container.container_name,
        blob_name=blob_name,
        account_key=storage_account_key(),
        expiry=datetime.datetime.now() + datetime.timedelta(days=10),
        permission="rd",
    )


@fixture(scope="session")
def resource_management_client() -> ResourceManagementClient:
    return _get_client(ResourceManagementClient)


@fixture(scope="session")
def storage_management_client() -> StorageManagementClient:
    return _get_client(StorageManagementClient)


@fixture(scope="session")
def data_factory_client() -> DataFactoryManagementClient:
    return _get_client(DataFactoryManagementClient)


@fixture(scope="session")
def data_lake_credential(
    storage_management_client: StorageManagementClient,
    request
) -> Callable[[], str]:
    resource_id = request.config.getoption("datalake_id")[0].split("/")

    def _data_lake_credential():
        return storage_management_client.storage_accounts.list_keys(
            resource_id[4],
            resource_id[8]
        ).keys[0].value

    return _data_lake_credential


@fixture(scope="session")
def file_system_client(
    data_lake_credential: Callable[[], str],
    unique_identifier: str,
    request
) -> FileSystemClient:
    resource_id = request.config.getoption("datalake_id")[0].split("/")
    account_url = f"https://{resource_id[8]}.dfs.core.windows.net"

    client = DataLakeServiceClient(
        account_url=account_url,
        credential=data_lake_credential()
    )

    try:
        fs: FileSystemClient = client.create_file_system(file_system=unique_identifier)
    except ResourceExistsError:
        fs: FileSystemClient = client.get_file_system_client(file_system=unique_identifier)

    yield fs
    fs.delete_file_system()


@fixture(scope="session")
def data_lake_directory(
    data_lake_credential: Callable[[], str],
    file_system_client: FileSystemClient
) -> DataLakeDirectoryClient:
    directory = DataLakeDirectoryClient(
        account_url=f"https://{file_system_client.account_name}.dfs.core.windows.net",
        file_system_name=file_system_client.file_system_name,
        directory_name="/",
        credential=data_lake_credential()
    )
    directory.set_access_control_recursive(
        "user::rwx,group::rwx,other::rwx,default:user::rwx,default:group::rwx,default:other::rwx"
    )
    return directory


@fixture(scope="session")
def databricks_cluster(
    resource_management_client: ResourceManagementClient,
    unique_identifier: str,
    request
) -> dict:
    resource_id = request.config.getoption("databricks_id")[0]
    workspace: GenericResource = resource_management_client.resources.get_by_id(resource_id, api_version="2018-04-01")
    url = workspace.properties.get("workspaceUrl")
    org_id = workspace.properties.get("workspaceId")
    resource = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    cred, _, _ = _get_credentials(resource)
    token = cred.get_token(resource).token

    client = ApiClient(host=f"https://{url}", token=token)
    api = ClusterApi(client)

    me = client.perform_query("GET", "/preview/scim/v2/Me")
    user_name = me.get("userName")

    cluster = api.create_cluster({
        "cluster_name": unique_identifier,
        "spark_version": "9.1.x-scala2.12",
        "node_type_id": "Standard_D3_v2",
        "num_workers": 1,
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "autotermination_minutes": 15,
        "spark_conf": {
            "spark.databricks.passthrough.enabled": "true"
        },
        "single_user_name": user_name
    })
    cluster_id = cluster.get("cluster_id")

    state = "PENDING"
    while state == "PENDING":
        time.sleep(1)
        cluster = api.get_cluster(cluster_id)
        state = cluster.get("state")

    if state != "RUNNING":
        raise Exception(cluster)

    cluster["sql_connection"] = {
        "server_hostname": url,
        "http_path": f"/sql/protocolv1/o/{org_id}/{cluster_id}",
        "access_token": token
    }

    yield cluster
    api.permanent_delete(cluster_id)


@fixture(scope="session")
def databricks_connection(
    databricks_cluster: dict
) -> Connection:
    sql_parameters = databricks_cluster.get("sql_connection")
    with sql.connect(**sql_parameters) as connection:
        yield connection


def _get_credentials(resource: str = None):
    cli_ctx = AzCli(
        config_dir=os.path.expanduser(
            os.environ.get("AZURE_CONFIG_DIR", "~/.azure")
        )
    )
    profile = Profile(cli_ctx=cli_ctx)
    if not resource:
        resource = cli_ctx.cloud.endpoints.resource_manager
    return profile.get_login_credentials(
        resource=resource
    )


def _get_client(client_type) -> Any:
    cred, subscription_id, tenant_id = _get_credentials()
    return client_type(
        credential=cred,
        subscription_id=subscription_id,
        tenant_id=tenant_id
    )
