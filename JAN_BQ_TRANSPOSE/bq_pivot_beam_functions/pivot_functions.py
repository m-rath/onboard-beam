import os
import re
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.pvalue import AsSingleton
from apache_beam.io.gcp.bigquery_tools import get_table_schema_from_string


PROJECT_ID = 'practice-springml'


def validate_table(args):
    bq_wrap = BigQueryWrapper()
    dataset, table_name = args.input_table_spec.split(r'.')
    try:
        bq_table = bq_wrap.get_table(PROJECT_ID, dataset, table_name)
    except:
        print(f"Table '{args.input_table_spec}' not found \n" \
            f"Please double-check project '{PROJECT_ID}' for table")
        return
    return bq_table


def validate_fields(bq_table, args):
    fd_names = [fd.name for fd in bq_table.schema.fields]
    target_cols = [*args.key_fields, *args.pivot_fields, *args.value_fields]
    if all(f in fd_names for f in target_cols):
        query_cols = ",".join([col for col in target_cols])
        query = rf'SELECT {query_cols} FROM {args.input_table_spec} WHERE name = "Drew";'
    else:
        print("Not all user-provided field names appear in specified table.")
        return
    # fd_schemas = [':'.join([fd.name, fd.type]) for fd in bq_table.schema.fields]
    return query #, fd_schemas


def schema_fn(destination, schema_str):
    """ structured to Beam's liking """
    return beam.io.gcp.bigquery_tools.get_table_schema_from_string(schema_str)