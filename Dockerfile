FROM python:3.10-slim

# Minimal system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY beam_pipeline ./beam_pipeline

# Entry point for Flex Template – run the pipeline module
ENTRYPOINT ["python", "-m", "beam_pipeline.bq_to_avro_and_csv"]
