from pyspark.sql import SparkSession
from pytest import fixture

import os
import shutil
import uuid


@fixture(scope="session")
def spark(location) -> SparkSession:
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-azure:3.3.1",
        )
        .config(
            "spark.sql.warehouse.dir", os.path.realpath(location)
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
def location() -> str:
    location = f"test-{uuid.uuid4()}"
    os.mkdir(location)
    yield location
    shutil.rmtree(location, ignore_errors=True)
