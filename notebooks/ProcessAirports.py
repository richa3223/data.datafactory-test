# Databricks notebook source
dbutils.widgets.text("SourceFileSystem", "")
dbutils.widgets.text("SourceFile", "")
dbutils.widgets.text("DestinationDatabase", "")
dbutils.widgets.text("DestinationTable", "")
dbutils.widgets.text("DestinationFileSystem", "")
dbutils.widgets.text("DestinationPath", "")

# COMMAND ----------

account = dbutils.secrets.get(scope="DatabricksSecrets", key="DataLakeName")

source_file_system = dbutils.widgets.get("SourceFileSystem")
source_filename = dbutils.widgets.get("SourceFile")
source_path = f"abfss://{source_file_system}@{account}.dfs.core.windows.net/{source_filename}"

database = dbutils.widgets.get("DestinationDatabase")
table = dbutils.widgets.get("DestinationTable")

dest_file_system = dbutils.widgets.get("DestinationFileSystem")
dest_path = dbutils.widgets.get("DestinationPath")
location = f"abfss://{dest_file_system}@{account}.dfs.core.windows.net/{dest_path}"

# COMMAND ----------

from airports.processor import AirportPipeline

pipeline = AirportPipeline(spark)
pipeline.run(source_path, database, table, location)

# COMMAND ----------

spark.table(f"{database}.{table}").display()
