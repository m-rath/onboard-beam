
import imp
import os
import re
import argparse
import logging
import datetime
from typing import TypedDict, NamedTuple
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.pvalue import AsSingleton
from apache_beam.io.gcp.bigquery_tools import get_table_schema_from_string

from bq_pivot_beam_classes import pivot_classes_schema, pivot_classes_records
from bq_pivot_beam_functions.pivot_functions import *


RUNNER = 'DataflowRunner'
PROJECT_ID = 'practice-springml'
REGION = 'us-central1'
PIPELINE_FOLDER = 'gs://dataflow-staging-us-central1-490138077fc741632143d4fcfb332271'

parser = argparse.ArgumentParser()
parser.add_argument('--input-table-spec', default = 'baby_names.orig_table')
parser.add_argument('--output-table-spec', default = 'baby_names.regional')
parser.add_argument('--key-fields', default = ['name', 'gender'], help = 'List[str]')
parser.add_argument(
    '--pivot-fields', 
    default = ['state'], 
    help = 'List[str]')
parser.add_argument('--value-fields', default = ['number'], help = 'List[str]')

args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    project = PROJECT_ID,
    runner = RUNNER,
    region = REGION,
    temp_location = f'{PIPELINE_FOLDER}/temp',
    staging_location = f'{PIPELINE_FOLDER}/staging',
    setup_file = r"C:\Users\markc\repos\onboard_beam\JAN_BQ_TRANSPOSE\setup.py",
    save_main_session = True
    )

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

# def register_schema(schema_str):

#     f_lists = [f.split(":") for f in schema_str.split(',')]
#     f_tuples = [((f[0], map2[f[1]])) for f in f_lists]
    
#     PivotedRecord = NamedTuple('PivotedRecord', f_tuples)
    
#     return PivotedRecord

# def schema_typed_dict(schema_str):
#     d = {}
#     for field_schema in schema_str.split(","):
#         k,v = field_schema.split(":")
#         d.update({k:map2[v]})
#     TD = TypedDict('td', d, total=False)
#     return TD
    

class StateToRegion(beam.DoFn):
    def __init__(self):
        self.regions = {
            'Northeast': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA'],
            'Midwest': ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
            'South': ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'LA', 'OK', 'TX'],
            'West': ['AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA']
            }
    def process(self, element):
        fin = {k:v for k,v in element.items() if k != 'state'}
        for region in self.regions:
            if element['state'] in self.regions[region]:
                # fin.update({'region': region})
                fin.update({'state': region})
                yield fin
        # if 'region' not in fin:
        if 'state' not in fin:
            # fin.update({'region': "other"})
            fin.update({'state': "other"})
            yield fin

#=================================================================

def run():

    # possible to skip validate_table() and validate_fields(),
    # if ReadFromBigQuery with table ref rather than query and 
    # with validate=True; but cheaper to read query than entire table
    bq_table = validate_table(args)

    # possible to skip validate_fields(),
    # then ReadFromBigQuery with table ref rather than query
    query = validate_fields(bq_table, args)

    with beam.Pipeline(options = pipeline_options) as p:

        # READ TABLE OR QUERY
        # orig_rows = p | "FromBQ" >> ReadFromBigQuery(
        #     table = ":".join(PROJECT_ID, args.input_table_spec), 
        #     validate = True)
        orig_rows = p | "FromBQ" >> ReadFromBigQuery(
            query = query, 
            project = PROJECT_ID)

        # ONE-TIME ONE-LINER
        rows = orig_rows | "StateToRegion" >> beam.ParDo(StateToRegion())
    
        # SCHEMA
        pivoted_schema_str = rows | pivot_classes_schema.PivotSchema(args)

        # PivotedRecord = register_schema(pivoted_schema_str)

        # RECORDS
        pivoted_records = rows | pivot_classes_records.PivotRecords(args)


        # 6) Write to BigQuery table sink.
        pivoted_records | 'Write' >> WriteToBigQuery(
            table = args.output_table_spec,
            schema = schema_fn,
            schema_side_inputs = (AsSingleton(pivoted_schema_str),),
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# #   Licensed to the Apache Software Foundation (ASF) under one
# #   or more contributor license agreements.  See the NOTICE file
# #   distributed with this work for additional information
# #   regarding copyright ownership.  The ASF licenses this file
# #   to you under the Apache License, Version 2.0 (the
# #   "License"); you may not use this file except in compliance
# #   with the License.  You may obtain a copy of the License at
# #
# #       http://www.apache.org/licenses/LICENSE-2.0
# #
# #   Unless required by applicable law or agreed to in writing, software
# #   distributed under the License is distributed on an "AS IS" BASIS,
# #   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #   See the License for the specific language governing permissions and
# #   limitations under the License.

# import apache_beam as beam

# from log_elements import LogElements
# class CollapsePivoted(beam.CombineFn):
#     """ 
#     this expects 1 key_field, of type string;
#     and sums all other fields, ints;
    
#     could be generalized to take a tuple of (row, operator)
#     """
#     def create_accumulator(self, keyfields):
#         return {}

#     def add_input(self, accumulator, input, keyfields):

#         for dct in input:
#             for k,v in dct.items():
#                 if k not in keyfields and type(v) != str: # if k in key_fields:
#                     if v is None: #necessary?
#                         v = 0
#                     if k not in accumulator:
#                         accumulator[k] = v
#                     else:
#                         accumulator[k] += v
#         return accumulator

#     def merge_accumulators(self, accumulators, keyfields):
#         merged = {}
#         for accum in accumulators:
#             for k, v in accum.items():
#                 if type(v) != str:
#                     if v is None:
#                         v = 0
#                     if k not in merged:
#                         merged[k] = v
#                     else:
#                         merged[k] += v
#         return merged

#     def extract_output(self, accumulator, keyfields):
#         return accumulator

# class ReconstrElem(beam.DoFn):
#   def process(self, element, keyfields):
#     key, val = element
#     keylist = key.split("__")
#     reconstr_elem = {k:v for k,v in zip(keyfields, keylist)}
#     reconstr_elem.update(val)
#     yield reconstr_elem

# class DeconstrElem(beam.DoFn):
#     """
#     Prepares multiple keyfields for serialized combinefn
#     """
#     def process(self, element, keyfields):
#         joint_key = "__".join([str(element[kfd]) for kfd in keyfields])
#         yield JointKeyField(joint_key), element

# @with_outputs(...)
# class JointKeyField(object):
#     def __init__(self):

#     ...https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/group_with_coder.py



# with beam.Pipeline() as p:

#   keyfields = ['name','gender']
#   (p | beam.Create([
#         {'name': 'Bobbie', 'gender': 3, 'number': 198, 'n':2},
#         {'name': 'Connie', 'gender': 3, 'number': 198, 'n':2},
#         {'name': 'Janet', 'gender': 3, 'number': 199, 'n':2},
#         {'name': 'Connie', 'gender': 3, 'number': 199, 'n':2}])
# #     | beam.GroupBy(lambda x: "__".join([str(x[kfd]) for kfd in keyfields]))
#      | beam.ParDo(DeconstrElem(), keyfields)
#      | beam.GroupByKey()
#      | beam.CombinePerKey(CollapsePivoted(), keyfields)
#      | beam.ParDo(ReconstrElem(), keyfields)
#      | LogElements())

# #---------------------------------------------------------------------------------

# # THIS WORKS!!!  note: the keyvalue cast to string for jointkfd reconstructs as string, even if it was int 

# import apache_beam as beam

# from log_elements import LogElements
# class CollapsePivoted(beam.CombineFn):
#     """ 
#     this takes 1 or more keyfields, of any type;
#     it sums all other fields, assumed numeric (so it accommodates 1 or more value_fields);
#     """
#     def create_accumulator(self, keyfields):
#         return {}

#     def add_input(self, accumulator, input, keyfields):

#         for dct in input:
#             for k,v in dct.items():
#                 if k not in keyfields and type(v) != str:
#                     if v is None:
#                         v = 0
#                     if k not in accumulator:
#                         accumulator[k] = v
#                     else:
#                         accumulator[k] += v
#         return accumulator

#     def merge_accumulators(self, accumulators, keyfields):
#         merged = {}
#         for accum in accumulators:
#             for k, v in accum.items():
#                 if type(v) != str:
#                     if v is None:
#                         v = 0
#                     if k not in merged:
#                         merged[k] = v
#                     else:
#                         merged[k] += v
#         return merged

#     def extract_output(self, accumulator, keyfields):
#         return accumulator

# class ReconstrElem(beam.DoFn):
#     """
#     two-tuple to dict
#     Tup[int, dict] --> Dict
#     """
#   def process(self, element, keyfields):
#     key, val = element
#     keylist = key.split("__")
#     reconstr_elem = {k:v for k,v in zip(keyfields, keylist)}
#     reconstr_elem.update(val)
#     yield reconstr_elem

# with beam.Pipeline() as p:

#   keyfields = ['name','gender']
#   (p | beam.Create([
#         {'name': 'Bobbie', 'gender': 3, 'number': 198, 'n':2},
#         {'name': 'Connie', 'gender': 3, 'number': 198, 'n':2},
#         {'name': 'Janet', 'gender': 3, 'number': 199, 'n':2},
#         {'name': 'Connie', 'gender': 3, 'number': 199, 'n':2}])
#      | beam.GroupBy(lambda x: "__".join([str(x[kfd]) for kfd in keyfields]))

#      | beam.CombinePerKey(CollapsePivoted(), keyfields)
#      | beam.ParDo(ReconstrElem(), keyfields)
#      | LogElements())

# # {'name': 'Bobbie', 'gender': '3', 'number': 198, 'n': 2}
# # {'name': 'Connie', 'gender': '3', 'number': 397, 'n': 4}
# # {'name': 'Janet', 'gender': '3', 'number': 199, 'n': 2}
