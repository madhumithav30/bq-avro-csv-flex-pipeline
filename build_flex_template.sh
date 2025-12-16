#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID=${PROJECT_ID:-"onyx-elevator-481414-b5"}
REGION=${REGION:-"asia-south1"}
BUCKET=${BUCKET:-"example-bucket-git"}

IMAGE="gcr.io/${PROJECT_ID}/bq-avro-csv-flex"
TEMPLATE_PATH="gs://${BUCKET}/templates/bq_avro_csv_flex.json"

echo "Building image ${IMAGE} in project ${PROJECT_ID}..."

gcloud builds submit . \
  --tag "${IMAGE}" \
  --project "${PROJECT_ID}"

echo "Building Flex Template at ${TEMPLATE_PATH}..."

gcloud dataflow flex-template build "${TEMPLATE_PATH}" \
  --image "${IMAGE}" \
  --sdk-language PYTHON \
  --metadata-file template/metadata.json \
  --project "${PROJECT_ID}" \
  --region "${REGION}"

echo "Done. Template: ${TEMPLATE_PATH}"
