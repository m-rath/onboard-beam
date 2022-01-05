"""
Mark Porath, Jan 4 2022

pivot or transpose, based on user input, from BQ source to BQ sink;
dynamic schema in beam python sdk is practically first of its kind :)

based on this java routine:
<https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-bigquery-transpose>

with this dataset <https://www.ssa.gov/OACT/babynames/limits.html>;
as prelim step, bq extract bigquery-public-data:usa_names.usa_1910_current
to practice-springml:baby_names.pivot_gender_state
"""

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

# from bq_T.extract_pivot_schema import ExtractPivot
# from bq_T.final_formatter import FormatPivotedRow


GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def run():

    PROJECT_ID = 'practice-springml'
    PIPELINE_FOLDER = 'gs://dataflow-staging-us-central1-490138077fc741632143d4fcfb332271'
    RUNNER = 'DataflowRunner'

    # User-provided variables
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-table-spec',
        default = 'baby_names.orig_table',
        help = "BigQuery <dataset>.<table> to query and transpose,"
        "ex: 'bnb_dataset.bnb_table'.")
    parser.add_argument(
        '--output-table-spec',
        default = 'baby_names.pivoted_table',
        help = "BigQuery destination for transposed output,"
        "ex: 'bnb_dataset.new_table'")
    parser.add_argument(
        '--key-fields',
        default = ['name'],
        help = "Comma separated list of key field names.")
    parser.add_argument(
        '--pivot-fields',
        default = ['gender', 'state'],
        help = "Comma separated list of pivot field names.")
    parser.add_argument(
        '--value-fields',
        default = ['number'],
        help = "Comma separated list of value field names.")

    args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        # job_name = 'practice-MP',
        # template_location = 'gs://practice-job/template',
        project = f'{PROJECT_ID}',
        runner = f'{RUNNER}',
        region = 'us-central1',
        temp_location = f'{PIPELINE_FOLDER}/temp',
        staging_location = f'{PIPELINE_FOLDER}/staging',
        save_main_session = True # so that workers can access imported modules
        )

    #------------------------------------------------------------------

    # unpack user-provided arguments
    dataset, input_table_name = args.input_table_spec.split(r'.')
    columns = [*args.key_fields, *args.pivot_fields, *args.value_fields]
    
    # make sure the table exists
    bq_wrap = BigQueryWrapper()
    try:
        table = bq_wrap.get_table('practice-springml',dataset,input_table_name)
    except:
        print("Table not found:"
        "Please double-check spelling of 'project_id' and 'input_table_spec'"
        f"Not found: {input_table_name}")
        return

    # get schema info from BQ source
    orig_field_schemas = [field.name+':'+field.type for field in table.schema.fields]

    fields = [field.name for field in table.schema.fields]
    if all(f in fields for f in columns):
        cols = ",".join([col for col in columns])
        query = rf'SELECT {cols} FROM {args.input_table_spec};'
    else:
        print("Not all user-provided field names appear in specified table.")
        return

    #--------------------------------------------------------------------------

    class ExtractSchemas(beam.DoFn):
        def process(self, element, k_fields, p_fields, v_fields, orig_field_schemas):

            # key_field schemas come from simple look-up
            for k_field in k_fields:
                key_field_schema = [x for x in orig_field_schemas if re.match(k_field, x)]
                yield key_field_schema[0] # ex: "host_name:STRING"

            # new schemas are prepared for a unified string format
            for p_field in p_fields:
                for v_field in v_fields:
                    col_name = element[p_field] + '_' + v_field
                    col_type = [x.split(":")[1] for x in orig_field_schemas if x.split(":")[0] == v_field]
                    new_field_schema = ":".join([col_name, *col_type]).replace(' ', '_')
                    yield new_field_schema # ex: "Staten_Island_room_type:STRING"

    class PivotRow(beam.DoFn):
        def process(self, element, k_fields, p_fields, v_fields):
            
            # start with the key_fields, unchanged
            pivoted_record = {k:v for k,v in element.items() if k in k_fields}
            
            # then update with new pivoted fields 
            for p_field in p_fields:
                for v_field in v_fields:
                    col_name = element[p_field] + '_' + v_field
                    col_name = col_name.replace(' ', '_')
                    value = element[v_field]
                    pivoted_record.update({col_name:value})

            yield pivoted_record

    class ExtractPivot(beam.PTransform):

        def __init__(self, k_fields, p_fields, v_fields, orig_field_schemas):
            self.k_fields = k_fields
            self.p_fields = p_fields
            self.v_fields = v_fields
            self.orig_field_schemas = orig_field_schemas

        def expand(self, orig_rows):
            field_schemas = orig_rows | beam.ParDo( ExtractSchemas(),
                self.k_fields,self.p_fields,self.v_fields,self.orig_field_schemas)

            records = orig_rows | beam.ParDo( PivotRow(),
                self.k_fields,self.p_fields,self.v_fields)

            return records, field_schemas

    class CollapsePivoted(beam.CombineFn):
        """ this expects 1 key_field, of type string;
        and sums all other fields, ints """
        def create_accumulator(self):
            return {}

        def add_input(self, accumulator, input):
            # accumulator == {}
            # input == {name: 'Mark', F_number: None, M_number: 200, OH_number: None, TX_number: 51}
            # idea  == {name: 'Mark', F_number: 1,    M_number: 200, OH_number: 150,  TX_number: 51}
            for row in input:
                for k,v in row.items():
                    if type(v) != str:
                        if v is None:
                            v = 0
                        if k not in accumulator:
                            accumulator[k] = v
                        else:
                            accumulator[k] += v
            return accumulator

        def merge_accumulators(self, accumulators):
            merged = {}
            for accum in accumulators:
                for k, v in accum.items():
                    if type(v) != str:
                        if v is None:
                            v = 0
                        if k not in merged:
                            merged[k] = v
                            merged[k] += v
            return merged

        def extract_output(self, accumulator):
            return accumulator

    #--------------------------------------------------------------------------

    with beam.Pipeline(options=pipeline_options) as p:

        # 1) Read from BigQuery table source.
        orig_rows = p | "Read BQ" >> ReadFromBigQuery(
            query=query, project=PROJECT_ID)

        # 2) Extract pivot schema from TableRow records.
        records, field_schemas = (orig_rows | ExtractPivot(
            args.key_fields, 
            args.pivot_fields, 
            args.value_fields, 
            orig_field_schemas)
        )

        # 3) Convert schema to string (later a singleton) for input to schema_fn...
        schema_str = (
            field_schemas 
            | beam.Distinct() 
            | beam.transforms.combiners.ToList()
            | beam.Map(lambda x: ",".join(x))
            )   

        # ...then convert schema_str to TableSchema (or use schema_fn for schema_str --> table_schema)...
        # table_schema = schema_str | beam.Map(beam.io.gcp.bigquery_tools.get_table_schema_from_string)

        # 4) Define schema function, necessary for user-driven staging of WriteToBigQuery sink
        def schema_fn(destination, schema_str):
            return beam.io.gcp.bigquery_tools.get_table_schema_from_string(schema_str)

        # 5) Format records (pivoted records will be sparse, many nulls).
        formatted_rows = (
            records
            | beam.GroupBy(lambda row: row.get(*args.key_fields))
            | beam.CombinePerKey(CollapsePivoted())
            | beam.Map(lambda tup: [ {args.key_fields[0] : tup[0]}, tup[1] ] )
            | beam.Map(lambda x: {k:v for d in x for k,v in d.items()} )
            )

        # 6) Write to BigQuery table sink.
        formatted_rows | 'Write' >> WriteToBigQuery(
            table = args.output_table_spec,
            schema = schema_fn, # would a lambda function work? or 'SCHEMA_AUTODETECT'?
            schema_side_inputs = (AsSingleton(schema_str),),
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()