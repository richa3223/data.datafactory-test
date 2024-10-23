from airports.processor import AirportPipeline

import pandas as pd
import os


def test_processor(spark, location):
    # arrange
    source = os.path.join(location, "source.csv")
    destination = os.path.join(location, "database")
    database = "raw"
    table = "airports"

    df = pd.DataFrame({
        "id": [1]
    })
    df.to_csv(source, header=False, index=False)

    # act
    pipeline = AirportPipeline(spark)
    pipeline.run(source, database, table, destination)
    rows = spark.table(f"{database}.{table}").collect()

    # assert
    assert len(rows) == len(df.index)
