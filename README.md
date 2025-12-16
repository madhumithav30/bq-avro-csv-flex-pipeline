# BigQuery → Avro + CSV (Dynamic Schema, Dataflow Flex Template)

Apache Beam (Python) pipeline that reads from a BigQuery table, applies simple filtering, writes Avro files to Cloud Storage using a **dynamically generated Avro schema**, and then converts those Avro files to CSV. The pipeline is designed to be packaged as a Dataflow Flex Template so it can be reused across different tables by changing only parameters.

## What the pipeline does

- Reads from a BigQuery table using `ReadFromBigQuery` given dataset and table parameters.
- Uses the `google-cloud-bigquery` client to fetch the table schema at runtime and converts it to an Avro record schema (`bq_schema_to_avro_schema`).
- Supports an optional equality filter on any column (for example, `country = 'CA'`) to limit which rows are exported.
- Writes the filtered rows to Avro files in GCS using the dynamically generated Avro schema (types are mapped via `BQ_TO_AVRO_TYPE`, including logical types for TIMESTAMP and NUMERIC).
- Reads those Avro files back and writes sharded CSV files with:
  - configurable subset and ordering of columns (`--csv_columns`)
  - configurable delimiter (`,` or `|`).
- Optionally writes dead‑letter records (if you enable that side output) as JSON lines into a `_dead_letter` folder for inspection.

Because the Avro schema is built from the BigQuery schema, the same pipeline can work for many tables, as long as their field types are covered by `BQ_TO_AVRO_TYPE`.

## Tech stack

- Apache Beam (Python SDK)
- Google Cloud Dataflow (Flex Template compatible)
- BigQuery (`ReadFromBigQuery` + `google-cloud-bigquery` client)
- Cloud Storage
- fastavro for Avro schema parsing and writing

## Running locally (DirectRunner)

From the repo root, after installing requirements:

PROJECT_ID=your-gcp-project-id
REGION=asia-south1
BUCKET=your-gcs-bucket

python beam_pipelines/bq_to_avro_pipeline.py
--project "${PROJECT_ID}"
--region "${REGION}"
--runner DirectRunner
--temp_location "gs://${BUCKET}/tmp"
--bq_dataset "your_dataset"
--bq_table "your_table"
--output_prefix "gs://${BUCKET}/exports/local_test"
--filter_column "country"
--filter_value "CA"
--csv_columns "txn_id,customer_id,txn_ts,country,amount,currency"
--csv_delimiter ","
--num_shards 2

After a successful run you should see:

- Avro files under `gs://your-bucket/exports/local_test-00000-of-00002.avro` (names depend on `num_shards`).
- CSV files under `gs://your-bucket/exports/local_test_csv/output-00000-of-00002.csv`.
- Dead‑letter JSON only if you keep the dead‑letter side output active.

## Build Docker image

From the repo root (where `docker/Dockerfile` exists):

```PROJECT_ID=your-project-id
IMAGE="gcr.io/${PROJECT_ID}/bq-avro-csv-flex"

gcloud builds submit .
--tag "${IMAGE}"
--project "${PROJECT_ID}" 
```

This builds and pushes the container image for your Flex Template.

## Build Flex Template

```
PROJECT_ID=your-project-id
REGION=your-region
BUCKET=your-bucket-name
IMAGE="gcr.io/${PROJECT_ID}/bq-avro-csv-flex"
TEMPLATE_PATH="gs://${BUCKET}/templates/bq_avro_csv_flex.json"

gcloud dataflow flex-template build "${TEMPLATE_PATH}"
--image "${IMAGE}"
--sdk-language PYTHON
--metadata-file template/metadata.json
--project "${PROJECT_ID}"
--region "${REGION}"
```

The resulting JSON in GCS defines a reusable Flex Template.

## Run Flex Template

```
JOB_NAME=bq-avro-csv-$(date +%Y%m%d-%H%M%S)
PROJECT_ID=your-project-id
REGION=your-region
BUCKET=your-bucket-name
TEMPLATE_PATH="gs://${BUCKET}/templates/bq_avro_csv_flex.json"

gcloud dataflow flex-template run "${JOB_NAME}"
--project "${PROJECT_ID}"
--region "${REGION}"
--template-file-gcs-location "${TEMPLATE_PATH}"
--parameters bq_dataset="your_dataset"
--parameters bq_table="your_table"
--parameters output_prefix="gs://${BUCKET}/exports/run1"
--parameters filter_column="country"
--parameters filter_value="CA"
--parameters csv_columns="txn_id,customer_id,txn_ts,country,amount,currency"
--parameters csv_delimiter=","
--parameters num_shards="5"
```

This runs the same dynamic‑schema pipeline on Dataflow, with parameters you can adjust per run.

