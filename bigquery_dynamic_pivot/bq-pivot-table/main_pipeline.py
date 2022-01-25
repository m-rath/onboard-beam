

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.pvalue import AsSingleton

from bq_pivot_beam_classes import pivot_classes_schema, pivot_classes_records
from bq_pivot_beam_functions.pivot_functions import *
# from bq_pivot_beam_classes.preprocessing import StateToRegion

RUNNER = 'DataflowRunner'
PROJECT_ID = 'practice-springml'
REGION = 'us-central1'
PIPELINE_FOLDER = 'gs://dataflow-staging-us-central1-490138077fc741632143d4fcfb332271'

parser = argparse.ArgumentParser()
parser.add_argument('--input-table-spec', default = 'baby_names.pivot_regional1')
parser.add_argument('--output-table-spec', default = 'baby_names.pivot_regional21')
parser.add_argument('--key-fields', default = ['name'], help = 'List[str]')
parser.add_argument(
    '--pivot-fields', 
    default = ['gender'], 
    help = 'List[str]')
parser.add_argument('--value-fields', default = ['South','Northeast','West','Midwest'], help = 'List[str]')

args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    project = PROJECT_ID,
    runner = RUNNER,
    region = REGION,
    temp_location = f'{PIPELINE_FOLDER}/temp',
    staging_location = f'{PIPELINE_FOLDER}/staging',
    setup_file = r"C:\Users\markc\repos\onboard_beam\bq-pivot-table\setup.py",
    save_main_session = True
    )


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

        pcoll =(
            p 
            | "FromBQ" >> ReadFromBigQuery(query = query, project = PROJECT_ID)
            # | "StateToRegion" >> beam.ParDo(StateToRegion())
        )
    
        new_schema_str, PivotedRecord = (
            pcoll 
            | pivot_classes_schema.PivotSchema(args)
            # | "Rename Columns1" >> beam.Map(
                # lambda x: x.replace("_number", ""))
        )

        # PivotedRecord = pivoted_schema_str | beam.Map(register_schema)
        # PivotedRecord = register_schema(pivoted_schema_str)
        # beam.coders.registry.register_coder(PivotedRecord, PivotedRecordCoder)

        pivoted_records = (
            pcoll 
            | pivot_classes_records.PivotRecords(args)
            # | "Rename Columns2" >> beam.Map(
                # lambda x: {k.replace("_number", ""):v for k,v in x.items()})
            | beam.Map(fill_fields, AsSingleton(PivotedRecord)) # this could be new_schema_str as side input, slight change in fill_fields()
            | beam.Map(lambda x: PivotedRecord(**x))#.with_output_types(PivotedRecord)
        )


        # _pickle.PicklingError: Can't pickle <class 'bq_pivot_beam_functions.pivot_functions.PivotedRecord'>: it's not the same object as bq_pivot_beam_functions.pivot_functions.PivotedRecord [while running 'Map(register_schema)']
        # ...
        # _pickle.PicklingError: Can't pickle <class 'bq_pivot_beam_functions.pivot_functions.PivotedRecord'>: attribute lookup PivotedRecord on bq_pivot_beam_functions.pivot_functions failed [while running 'Map(register_schema)']

        pivoted_records | 'Write' >> WriteToBigQuery(
            table = args.output_table_spec,
            schema = schema_fn,
            schema_side_inputs = (AsSingleton(new_schema_str),),
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
