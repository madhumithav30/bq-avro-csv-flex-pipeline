import argparse
import csv
import io
import json
import logging
from typing import Dict, Any, List
from google.cloud import bigquery  # dynamic schema fetch
from fastavro import parse_schema  # optional but recommended [web:189]

import apache_beam as beam
from apache_beam.io.avroio import WriteToAvro, ReadFromAvro
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    GoogleCloudOptions,
    StandardOptions,
)

# AVRO_SCHEMA = {
#     "type": "record",
#     "name": "Transaction",
#     "fields": [
#         {"name": "txn_id",      "type": ["null", "long"],   "default": None},
#         {"name": "customer_id", "type": ["null", "long"],   "default": None},
#         {"name": "txn_ts","type": ["null", {"type": "long", "logicalType": "timestamp-micros"}],"default": None},
#         {"name": "country",     "type": ["null", "string"], "default": None},
#         {
#             "name": "amount",
#             "type": [
#                 "null",
#                 {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9}
#             ],
#             "default": None
#         },
#         {"name": "currency",    "type": ["null", "string"], "default": None},
#         {"name": "revenue_band","type": ["null", "string"], "default": None}
#     ]
# }
BQ_TO_AVRO_TYPE = {
    "STRING": "string",
    "BYTES": "bytes",
    "INTEGER": "long",
    "INT64": "long",
    "FLOAT": "double",
    "FLOAT64": "double",
    "NUMERIC": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9},
    "BIGNUMERIC": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 38},
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "TIMESTAMP": {"type": "long", "logicalType": "timestamp-micros"},
    "DATE": "string",
    "TIME": "string",
    "DATETIME": "string"
    # You can extend this map for RECORD/STRUCT, GEOGRAPHY, etc. [web:191][web:194]
}

def bq_schema_to_avro_schema(
    table_schema: list,
    record_name: str = "Record"
) -> dict:
    """Convert BigQuery table schema (list of SchemaField) to an Avro record schema."""
    fields = []
    for field in table_schema:
        bq_type = field.field_type.upper()
        avro_type = BQ_TO_AVRO_TYPE.get(bq_type, "string")

        # Wrap in union with null so fields can be missing.
        if isinstance(avro_type, dict):
            avro_union = ["null", avro_type]
        else:
            avro_union = ["null", avro_type]

        fields.append(
            {
                "name": field.name,
                "type": avro_union,
                "default": None,
            }
        )


    schema = {"type": "record", "name": record_name, "fields": fields}
    return parse_schema(schema)  

class BqToAvroCsvOptions(PipelineOptions):
    """Options for BigQuery → Avro + CSV with basic transformations."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--bq_dataset",
            help="BigQuery dataset ID, e.g. Test",
            required=True,
        )
        parser.add_argument(
            "--bq_table",
            help="BigQuery table ID, e.g. transactions",
            required=True,
        )
        parser.add_argument(
            "--output_prefix",
            help="Base GCS prefix, e.g. gs://bucket/exports/demo",
            required=True,
        )
        parser.add_argument(
            "--csv_columns",
            help="Comma-separated list of columns to keep in CSV (in order). "
                 "If not set, all columns are written.",
            default=None,
        )
        parser.add_argument(
            "--filter_column",
            help="Optional column name used for simple equality filter, e.g. country.",
            default=None,
        )
        parser.add_argument(
            "--filter_value",
            help="Optional value for filter_column, e.g. CA.",
            default=None,
        )
        parser.add_argument(
            "--amount_column",
            help="Numeric column used to create a derived 'revenue_band' field.",
            default=None,
        )
        parser.add_argument(
            "--csv_delimiter",
            help="Delimiter for CSV output, default ','. Use '|' for pipe.",
            default=",",
        )
        parser.add_argument(
            "--num_shards",
            type=int,
            default=5,
            help="Number of shards for Avro and CSV outputs.",
        )


class TransformAndValidate(beam.DoFn):
    """Apply filters, derive fields, and route bad rows to dead-letter."""

    def __init__(self, filter_col: str | None, filter_val: str | None, amount_col: str | None):
        self.filter_col = filter_col
        self.filter_val = filter_val
        self.amount_col = amount_col

        self.rows_in = Metrics.counter(self.__class__, "rows_in")
        self.rows_out = Metrics.counter(self.__class__, "rows_out")
        self.rows_filtered = Metrics.counter(self.__class__, "rows_filtered")
        self.rows_invalid = Metrics.counter(self.__class__, "rows_invalid")

    def process(self, row: Dict[str, Any]):
        self.rows_in.inc()

        if self.filter_col and self.filter_val is not None:
            if row.get(self.filter_col) != self.filter_val:
                self.rows_filtered.inc()
                return

        if self.amount_col:
            amount_val = row.get(self.amount_col)
            try:
                if amount_val is not None:
                    amount = float(amount_val)
                    if amount < 1000:
                        band = "LOW"
                    elif amount < 10000:
                        band = "MEDIUM"
                    else:
                        band = "HIGH"
                    row["revenue_band"] = band
            except Exception:
                # Just log and continue; don't dead-letter the whole row
                logging.warning("Could not compute revenue_band for amount=%r", amount_val)
                # do NOT yield to dead_letter here

                return

        self.rows_out.inc()
        yield row


class DictToCsvLine(beam.DoFn):
    """Convert a dict row to a CSV line with selected columns and delimiter."""

    def __init__(self, csv_columns: str | None, delimiter: str | None):
        self.csv_columns = csv_columns
        self.delimiter = delimiter or ","

    def process(self, row: Dict[str, Any]):
        if self.csv_columns:
            columns: List[str] = [c.strip() for c in self.csv_columns.split(",")]
        else:
            columns = sorted(row.keys())

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=self.delimiter)
        writer.writerow([row.get(col, "") for col in columns])
        yield buf.getvalue().rstrip("\n")


def run(argv=None):
    pipeline_options = BqToAvroCsvOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    std_opts = pipeline_options.view_as(StandardOptions)
    if not std_opts.runner:
        std_opts.runner = "DirectRunner"  # safe for local test

    gcloud_opts = pipeline_options.view_as(GoogleCloudOptions)
    project_id = gcloud_opts.project
    dataset_id = pipeline_options.bq_dataset
    table_id = pipeline_options.bq_table

     # Build table spec and fetch schema dynamically
    table_spec = f"{project_id}.{dataset_id}.{table_id}"  # for BigQuery client
    logging.info("Fetching BQ schema for %s", table_spec)

    bq_client = bigquery.Client(project=project_id)
    table = bq_client.get_table(table_spec)
    dynamic_avro_schema = bq_schema_to_avro_schema(table.schema, record_name="Transaction")
    logging.info(
        "Project: %s, Region: %s, table_spec=%s",
        project_id,
        gcloud_opts.region,
        f"{project_id}:{dataset_id}.{table_id}",
    )


    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p
            | "ReadFromBigQuery"
            >> ReadFromBigQuery(
                table=table_spec,
                method="DIRECT_READ",
            )
        )

        transformed, dead_letter = (
            rows
            | "TransformAndValidate"
            >> beam.ParDo(
                TransformAndValidate(
                    filter_col=pipeline_options.filter_column,
                    filter_val=pipeline_options.filter_value,
                    amount_col=pipeline_options.amount_column,
                )
            ).with_outputs("dead_letter", main="main")
        )

        _ = (
            dead_letter
            | "DeadLetterToJson"
            >> beam.Map(json.dumps)
            | "WriteDeadLetter"
            >> beam.io.WriteToText(
                file_path_prefix=pipeline_options.output_prefix + "_dead_letter/errors",
                file_name_suffix=".json",
                shard_name_template="-SSSSS-of-NNNNN",
            )
        )

        _ = (
            transformed
            | "WriteAvro"
            >> WriteToAvro(
                file_path_prefix=pipeline_options.output_prefix,
                file_name_suffix=".avro",
                schema=dynamic_avro_schema,
                num_shards=pipeline_options.num_shards,
                shard_name_template="-SSSSS-of-NNNNN",
            )
        )



        avro_pattern = pipeline_options.output_prefix + "*.avro"

        avro_rows = (
            p
            | "ReadAvroBack"
            >> ReadFromAvro(avro_pattern)
        )

        csv_lines = avro_rows | "ToCsvLines" >> beam.ParDo(
            DictToCsvLine(
                csv_columns=pipeline_options.csv_columns,
                delimiter=pipeline_options.csv_delimiter,
            )
        )

        _ = (
            csv_lines
            | "WriteCsv"
            >> beam.io.WriteToText(
                file_path_prefix=pipeline_options.output_prefix + "_csv/output",
                file_name_suffix=".csv",
                num_shards=pipeline_options.num_shards,
                shard_name_template="-SSSSS-of-NNNNN",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
