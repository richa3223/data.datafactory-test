FROM ubuntu:latest

COPY ./requirements.txt .

RUN apt-get update \
 && apt-get install -qq -y python3.8 python3-pip curl gnupg2 \
 && python3.8 -m pip install -q wheel \
 && python3.8 -m pip install -q -r requirements.txt

ARG DATABRICKS_HOST
ARG DATABRICKS_HTTP_PATH
EXPOSE 8000

COPY ./data-api/hosting.py .

ENV DATABRICKS_HOST=${DATABRICKS_HOST}
ENV DATABRICKS_HTTP_PATH=${DATABRICKS_HTTP_PATH}
CMD ["uvicorn", "--host", "0.0.0.0", "hosting:app"]