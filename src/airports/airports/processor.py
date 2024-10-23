from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp


class AirportPipeline:
    """The pipeline class used to transform airport data."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def _transform(self, df: DataFrame) -> DataFrame:
        return df.replace("\\N", None)

    def _add_processing_timestamp(self, df: DataFrame) -> DataFrame:
        return df.withColumn("processing_date", current_timestamp())

    def run(
        self, source: str, database: str, table: str, location: str
    ) -> None:
        """Executes the data pipeline.

        Args:
            self:       The airport pipeline instance
            source:     The source file to process
            database:   The database name
            table:      The target table name
            location:   The location of the database
        """
        schema = StructType(
            [
                StructField("airport_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("iata", StringType(), True),
                StructField("icao", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("altitude", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("dst", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("type", StringType(), True),
                StructField("source", StringType(), True),
            ]
        )

        df = self.spark.read.csv(
            source, header=False, inferSchema=False, schema=schema
        )
        df = (
            df.transform(self._transform)
              .transform(self._add_processing_timestamp)
        )

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS `{database}` LOCATION '{location}'"
        )
        df.write.saveAsTable(
            f"`{database}`.`{table}`", format="delta", mode="overwrite"
        )
