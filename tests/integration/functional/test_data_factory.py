from pytest_bdd import given, when, then, parsers, scenarios
from azure.storage.blob import ContainerClient, BlobClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import CreateRunResponse, PipelineRun
from azure.multiapi.storagev2.filedatalake.v2020_02_10 import (
    DataLakeDirectoryClient,
    FileSystemClient,
)
from databricks.sql.client import Connection
from threading import Lock
from pytest import fixture

from typing import Any, Callable, Tuple

import pandas as pd
import uuid
import time
import io
import os


scenarios(
    os.path.join(
        *os.path.split(os.path.realpath(__file__))[:-1],
        "../features/data_factory.feature"
    )
)


@fixture(name="lock", scope="module")
def lock_fixture() -> Lock:
    return Lock()


@fixture(scope="module")
def feature_context() -> Any:
    class _context:
        def __init__(self):
            self.dataset = None
    return _context()


@given(parsers.parse("there are airports:\n{table}"), target_fixture="dataset")
def given_airports(
    feature_context: Any,
    lock: Lock,
    table: str
) -> pd.DataFrame:
    """
    """
    with lock:
        if feature_context.dataset is None:
            with io.StringIO(table) as sb:
                feature_context.dataset = (
                    pd.read_csv(sb, sep=r"\s*\|\s*", header=None)
                      .iloc[:, 1:-1]
                      .rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
                )
        return feature_context.dataset


@given(parsers.parse("a source csv file"), target_fixture="source")
def given_a_source_csv_file(
    dataset: pd.DataFrame,
    blob_container: ContainerClient,
    generate_sas: Callable[[str], str],
) -> Tuple[str, str]:
    """
    """
    with io.StringIO() as buffer:
        dataset.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        blob: BlobClient = blob_container.upload_blob(
            name=f"{uuid.uuid4()}.dat",
            data=buffer.read().encode("utf-8"),
            overwrite=True,
        )
        sas = generate_sas(blob_name=blob.blob_name)
        url = f"{blob.url}?{sas}"
        parts = url.split("/")
        sourceBaseUrl = str.join("/", parts[:3])
        sourceRelativeUrl = str.join("/", [""] + parts[3:])

        return (sourceBaseUrl, sourceRelativeUrl)


@when(parsers.parse("it is transformed"), target_fixture="run")
def when_it_is_transformed(
    data_factory_client: DataFactoryManagementClient,
    data_lake_directory: DataLakeDirectoryClient,
    unique_identifier: str,
    source: Tuple[str, str],
    request: Any
) -> PipelineRun:
    parameters = {
        "sourceBaseUrl": source[0],
        "sourceRelativeUrl": source[1],
        "rawFileSystem": data_lake_directory.file_system_name,
        "rawPath": "airports",
        "queryFileSystem": data_lake_directory.file_system_name,
        "queryPath": "databases",
        "queryDatabase": unique_identifier,
        "queryTable": "airports"
    }

    pipeline = "AirportPipeline"
    resource_id = request.config.getoption("datafactory_id")[0].split("/")
    response: CreateRunResponse = data_factory_client.pipelines.create_run(
        resource_group_name=resource_id[4],
        factory_name=resource_id[8],
        pipeline_name=pipeline,
        parameters=parameters
    )

    status = "QUEUED"
    while status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        time.sleep(5)
        run: PipelineRun = data_factory_client.pipeline_runs.get(
            resource_group_name=resource_id[4],
            factory_name=resource_id[8],
            run_id=response.run_id
        )
        status = run.status.upper()

    return run


@then(parsers.parse("a table is created"))
def then_a_table_is_created(
    file_system_client: FileSystemClient,
    databricks_connection: Connection,
    dataset: pd.DataFrame,
    run: PipelineRun
):
    assert run.status.upper() == "SUCCEEDED"

    paths = [p.name for p in file_system_client.get_paths("/", recursive=False)]
    assert run.parameters.get("rawPath") in paths
    assert run.parameters.get("queryPath")

    with databricks_connection.cursor() as sql:
        try:
            sql.execute("SHOW DATABASES")
            databases = [d[0] for d in sql.fetchall()]
            database = run.parameters.get("queryDatabase")
            assert database in databases

            sql.execute(f"USE {database}")
            sql.execute("SHOW TABLES")
            tables = [t[1] for t in sql.fetchall()]
            table = run.parameters.get("queryTable")
            assert table in tables

            sql.execute(f"SELECT * FROM {database}.{table}")
            columns = [c[0] for c in sql.description]
            df = pd.DataFrame(sql.fetchall(), columns=columns)
            assert len(dataset.index) == len(df.index)
        finally:
            sql.execute(f"DROP DATABASE {database} CASCADE")
