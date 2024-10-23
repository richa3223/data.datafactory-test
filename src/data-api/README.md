# Data API
This Python module hosts an API to serve the airport data.

## Build

```bash
docker build \
    --build-arg DATABRICKS_HOST=adb-1234567890123456.78.azuredatabricks.net \
    --build-arg DATABRICKS_HTTP_PATH=/sql/protocolv1/o/1234567890123456/12345-12345-12345 \
    -t airports-api .
```

```bash
docker run --rm -it -p 8000:8000 airports-api
```

```bash
token=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -otsv)
curl -H "Authorization: Bearer $token" http://localhost:8000/airports
```