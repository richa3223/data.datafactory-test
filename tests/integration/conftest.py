import os


def pytest_addoption(parser):
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
            "--secret-scope",
            nargs=1,
            action="store",
            type=str,
            help="Databricks resource ID",
            default=[os.environ.get("SECRET_SCOPE")]
        )
    except ValueError:
        pass
