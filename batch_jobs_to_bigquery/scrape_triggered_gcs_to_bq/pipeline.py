
import re
import logging
from copy import copy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery, ReadFromText


OUTPUT_DATASET = "twitter_pulse"

class TriggerParams(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--input') 

pipeline_options = PipelineOptions([
    '--project', "practice-springml-east",
    '--region', "us-east1",
    '--runner', "DataflowRunner",
    '--template_location', "gs://twitter-pipeline-template/dataflow-pipeline",
    '--staging_location', "gs://twitter-pipeline-extras/staging",
    '--temp_location', "gs://twitter-pipeline-extras/temp",
    '--save_main_session'
    ])

trigger_params = pipeline_options.view_as(TriggerParams)

fields = ["neg","neu","pos","compound",
    "polarity","subjectivity","word_sample","tok2vec","tweet_id","tweet"]

def dictify(elem, fields):
    row = copy(elem)
    p = re.compile(r"""(?xs:\d+,(.*\.\d+),(.*\.\d+),(.*\.\d+),(.*\.\d+),
        (.*\.\d+),(.*\.\d+),(.*\[.*\].*),.(\[.*\]).,(\d+),(.*))""")                        
    m = p.findall(row)
    if len(m)==0:
        row_vals = [None] * len(fields)
    else:
        row_vals = list(m[0])
    return { k.split(":")[0]:v for k,v in zip(fields, row_vals) }

def cast_nums(elem):
    row = copy(elem)
    for field in ['neg','neu','pos','compound','polarity','subjectivity']:
        if row[field]:
            row[field] = float(row[field])
    if row['tweet_id']:
        row['tweet_id'] = int(row['tweet_id'])
    return row

def clean_embeddings(elem):
    row = copy(elem)
    if row['tok2vec']:
        vector = row['tok2vec'][1:-1].replace('\'','').split(',')
        row['tok2vec'] = [float(t.strip()) for t in vector]
    return row

def clean_words(elem):
    row = copy(elem)
    if isinstance(row['word_sample'], str):
        words = row['word_sample']
        row['word_sample'] = list(
            map(lambda x: re.sub("\W", "", x), words.split(',')))
    if row['tweet']:
        row['tweet'] = row['tweet'].strip("\"").rstrip().rstrip("\"").rstrip()
    return row

def by_vader_compound(row, num_partitions):
    if row['compound']:
        if row['compound'] < -0.5:
            return 0
        elif row['compound'] > 0.6:
            return 1
        else:
            return 2
    else:
        return 2


def run():

    with beam.Pipeline(options = pipeline_options) as p:

        pc_neg, pc_pos, __ = (
            p 
            | ReadFromText(trigger_params.input, skip_header_lines=1)
            | "parse" >> beam.Map(dictify, fields)
            | "clean embeddings" >> beam.Map(clean_embeddings)
            | "clean words" >> beam.Map(clean_words)
            | "cast nums" >> beam.Map(cast_nums)
            | "partition" >> beam.Partition(by_vader_compound, 3)
        )

        pc_pos | "Write Pos" >> WriteToBigQuery(
            table = OUTPUT_DATASET + r".pos",
            schema = 'SCHEMA_AUTODETECT',
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_APPEND')

        pc_neg | "Write Neg" >> WriteToBigQuery(
            table = OUTPUT_DATASET + r".neg",
            schema = 'SCHEMA_AUTODETECT',
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_APPEND')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()