
import os
import re
import argparse
import logging
from copy import copy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from io import TextIOWrapper
from csv import DictReader
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO


# PROJECT_ID = os.getenv("PROJECT_ID")
# PIPELINE_FOLDER = os.getenv("PIPELINE_FOLDER")
# # TEMPLATE_LOCATION = os.getenv("TEMPLATE_LOCATION")
# INPUT_FILE = os.getenv("INPUT_FILE")
# OUTPUT_DATASET = os.getenv("OUTPUT_DATASET")
PROJECT_ID = "practice-springml"
PIPELINE_FOLDER = "gs://dataflow-staging-us-central1-490138077fc741632143d4fcfb332271"
# TEMPLATE_LOCATION = "gs://twitter-template-dag/dataflow-template-jan27"
# INPUT_FILE = "gs://practice-springml.appspot.com/twitter_pulse 2022-01-10 095140.csv"
INPUT_FILE_PREFIX = "gs://practice-springml.appspot.com/twitter_pulse"
OUTPUT_DATASET = "twitter_pulse"

parser = argparse.ArgumentParser()
parser.add_argument('--project-id', default=PROJECT_ID)
parser.add_argument('--temp-location', default=f"{PIPELINE_FOLDER}/temp")
parser.add_argument('--staging-location', default=f"{PIPELINE_FOLDER}/staging")
# parser.add_argument('--template-location', default=TEMPLATE_LOCATION)
parser.add_argument('--input-file-prefix', default=INPUT_FILE_PREFIX)
parser.add_argument('--output-dataset', default=OUTPUT_DATASET)

args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    project = PROJECT_ID,
    runner = "DataflowRunner",
    region = 'us-central1',
    # template_location = args.template_location,
    temp_location = args.temp_location,
    staging_location = args.staging_location,
    save_main_session = True
    )

schema = [
    "tweet_id:STRING", 
    "tweet:STRING",
    "neg:FLOAT",
    "neu:FLOAT", 
    "pos:FLOAT",
    "compound:FLOAT",
    "polarity:FLOAT",
    "subjectivity:FLOAT",
    "word_sample:STRING",
    "tok2vec:STRING",
    ]
schema = ",".join([f for f in schema])


class LoadCSV():

    def __init__(self, gcs_path = args.input_file_prefix):
        self.client = GcsIO()
        bucket_list = self.client.list_prefix(gcs_path)
        self.file = list(bucket_list.keys())[0]

    def process(self):
        elements = []
        with self.client.open(self.file, mode = 'rb') as csv_file:
            csv_dict = DictReader(
                TextIOWrapper(csv_file, newline = '', errors = 'replace'))
            for elem in csv_dict:
                elements.append(elem)
        return elements


def cast_floats(elem):
    row = copy(elem)
    for field in ['neg','neu','pos','compound','polarity','subjectivity']:
        row[field] = float(row[field])
    return row


def clean_a_bit(elem):
    row = {k:v for k,v in elem.items()}
    for k,v in row.items():
        if type(k) == str:
            row[k] = row[k].replace('\n', '')
            row[k] = re.sub('\W{2,}', ' ', row[k])
    return row

def pop_off(elem):
    row = copy(elem)
    row.pop('')
    return row

def by_vader_compound(tweet, num_partitions):
    if 'compound' in tweet:
        if tweet['compound'] < -0.5:
            return 0
        elif tweet['compound'] > 0.6:
            return 1
        else:
            return 2


def run():

    loader = LoadCSV()
    csv_dicts = loader.process()

    with beam.Pipeline(options = pipeline_options) as p:

        pc_neg, pc_pos, __ = (
            p 
            | "Read" >> beam.Create(csv_dicts)
            | "Pop col 0" >> beam.Map(pop_off)
            | "Clean a bit" >> beam.Map(clean_a_bit)
            | "Cast" >> beam.Map(cast_floats)
            | "Sort" >> beam.Partition(by_vader_compound, 3)
        )

        pc_pos | "Write Pos" >> WriteToBigQuery(
            table = args.output_dataset + r".pos",
            schema = schema,
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_APPEND')

        pc_neg | "Write Neg" >> WriteToBigQuery(
            table = args.output_dataset + r".neg",
            schema = schema,
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_APPEND')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()