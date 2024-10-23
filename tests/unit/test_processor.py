from airports.processor import AirportPipeline
from pyspark.sql import Row


def test_airports_processor(spark):
    pipeline = AirportPipeline(spark)

    df = spark.createDataFrame([Row()])
    df = pipeline._transform(df)

    rows = df.collect()

    assert len(rows) == 1


def test_airports_processing_timestamp(spark):
    pipeline = AirportPipeline(spark)

    df = spark.createDataFrame([Row()])
    df = pipeline._add_processing_timestamp(df)

    assert "processing_date" in df.columns
