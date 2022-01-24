
import os
import re
import argparse
import logging
import datetime
from typing import TypedDict
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.pvalue import AsSingleton
from apache_beam.io.gcp.bigquery_tools import get_table_schema_from_string


class PivotSchema(beam.PTransform):

    def __init__(self, args):

        self.key_fds = args.key_fields
        self.piv_fds = args.pivot_fields
        self.val_fds = args.value_fields

    def expand(self, pcollection):

        pivoted_schema_str = (
            pcollection
            | beam.ParDo(
                FieldSchemas(), self.key_fds, self.piv_fds, self.val_fds)
            | beam.Distinct() 
            | beam.transforms.combiners.ToList()
            | beam.Map(lambda x: ",".join(x))
            )   
        return pivoted_schema_str


class FieldSchemas(beam.DoFn):
    
    def __init__(self):

        self.map = {
            str: "STRING",
            int: "INTEGER",
            float: "FLOAT",
            bytes: "BYTES",
            bool: "BOOLEAN",
            datetime.date: "DATE",
            datetime.time: "TIME",
            datetime.datetime: "DATETIME",
            datetime.datetime.timestamp: "TIMESTAMP",
            list: "STRUCT"
            }
    
    def process(self, elem, key_fds, piv_fds, val_fds):
        
        # key-field schemas remain unchanged
        for key_fd in key_fds:
            v = elem[key_fd]
            bq_type = self.map[type(v)] or "ANY"
            yield ":".join([key_fd, bq_type])

        # new schemas may differ from row to row
        for piv_fd in piv_fds:
            for val_fd in val_fds:
                col = '_'.join([str(elem[piv_fd]), val_fd]).replace(' ', '_')
                v = elem[val_fd]
                bq_type = self.map[type(v)] or "ANY"
                yield ":".join([col, bq_type])

# # defining and registering a TypedDict
# map = {
#     str: 'STRING',
#     int: 'INTEGER',
#     float: 'FLOAT',
#     bytes: 'BYTES',
#     bool: 'BOOLEAN',
#     datetime.date: 'DATE',
#     datetime.time: 'TIME',
#     datetime.datetime: 'DATETIME',
# }

# map2 = {v:k for k,v in map.items()}


# def schema_typed_dict(schema_str):
#     d = {}
#     for field_schema in schema_str.split(","):
#         k,v = field_schema.split(":")
#         d.update({k:map2[v]})
#     TD = TypedDict('td', d, total=False)
#     return TD