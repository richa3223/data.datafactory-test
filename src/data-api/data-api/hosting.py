from fastapi import FastAPI, Header
from databricks import sql
from typing import Optional

import pandas as pd
import uvicorn
import io
import os


app = FastAPI()


def execute_query(
    query: str,
    authorization: Optional[str],
    database: Optional[str] = None
) -> pd.DataFrame:
    """ Executes a SQL Query
    """
    host = os.environ.get("DATABRICKS_HOST")
    http = os.environ.get("DATABRICKS_HTTP_PATH")
    token = authorization.split(" ")[-1]
    with sql.connect(host, http, token) as conn:
        with conn.cursor() as cursor:
            if database is not None:
                cursor.execute(f"USE `{database}`")
            cursor.execute(query)

            desc = cursor.description
            rows = cursor.fetchall()
            return pd.DataFrame(
                rows,
                columns=[c[0] for c in desc]
            )


def dataframe_to_json(df: pd.DataFrame) -> str:
    with io.StringIO() as b:
        df.to_json(b, orient="records")
        b.seek(0)
        return b.read()


@app.get("/airports")
def get_airports(
    authorization: Optional[str] = Header(None)
) -> str:
    return dataframe_to_json(
        execute_query(
            "SELECT * FROM query.airports",
            authorization
        )
    )


if __name__ == "__main__":
    uvicorn.run(app)
