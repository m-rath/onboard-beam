""" 
Mark Porath
<https://www.youtube.com/watch?v=jU6EmPyKefg>

<https://beam.apache.org/releases/pydoc/2.34.0/apache_beam.transforms.core.html>

<https://docs.python.org/3/library/csv.html#reader-objects>


48895 flawless rows, with unwanted newline characters replaced by spaces;
BUT all in-memory, with python's csv library then simple beam.Create;
a perfect opportunity to learn Splittable DoFn (LoadCSV is ready to be a DoFn)

practice-springml:bnb19.bnb_table
practice-springml:bnb19.nhood_counts
"""

import argparse
import logging
from io import TextIOWrapper
from csv import DictReader
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io import WriteToBigQuery
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms.ptransform import _create_transform
# from apache_beam.DoFn import Re

#----------------------------------------------------------------------
# SPLITTABLE DOFN IS TOUGH, SO LET'S DO THIS IN MEMORY 

#--------------SET OPTIONS-----------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--input-csv', default = 'gs://airbnb-nyc-19/AB_NYC_2019.csv')
parser.add_argument('--main-bq-table', default='bnb_19_sdf.bnb_table')
parser.add_argument('--counts-bq-table', default='bnb_19_sdf.nhood_counts')

app_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    runner = 'DataflowRunner',
    project = 'practice-springml',
    region = 'us-central1',
    # job_name = 'practice-MP',
    temp_location = 'gs://practice-springml.appspot.com/tmp',
    # staging_location = 'gs://staging.practice-springml.appspot.com/stg',
    # template_location = 'gs://practice-job/template',
    save_main_session = True # so workers can access imported modules
    )

class PitchingCoach(RestrictionProvider):
    
    def __init__(self, gcs_path = app_args.input_csv):
        self.client = GcsIO()
        self.path = gcs_path
    
    def initial_restriction(self):
        start_stop = (0, self.client.size(self.path)) # (0, 7074793) for input_csv == AB_NYC_2019.csv
        return start_stop

    def create_tracker(self, restriction):
        tracker = OffsetRangeTracker(*restriction)
        # tracker = OffsetRestrictionTracker(OffsetRange(*restriction))
        return tracker

    # def restriction_size(self, element, restriction):
    #     """Returns the size of a restriction with respect to the given element.
    #     By default, asks a newly-created restriction tracker for the default size
    #     of the restriction."""
    #     #ok, so shot in the dark here...
    #     return tracker. fraction_claimed?

 
    # def split(self, restriction, num_parts):
    #     start, stop = restriction
    #     range_size = (stop - start) // num_parts
    #     current_start = start
    #     current_stop = stop
    #     while current_start <= current_stop:
    #         current_stop = min(current_start + range_size, stop)
    #         yield (current_start, current_stop)
    #         current_start = current_stop
    

# class MyRP(RestrictionProvider):
#   def initial_restriction(self, file_name):
#     return OffsetRange(0, os.stat(file_name).st_size)
#   def create_tracker(self, restriction):
#     return beam.io.restriction_trackers.OffsetRestrictionTracker()

# class FileToWordsFn(beam.DoFn):
#   def process(
#       self,
#       file_name,
#       tracker=beam.DoFn.RestrictionParam(MyRP())):
#     with open(file_name) as file_handle:
#       file_handle.seek(tracker.current_restriction.start())
#       while tracker.try_claim(file_handle.tell()):
#         yield read_next_record(file_handle)

class LoadCSV(beam.DoFn):

    def __init__(self, gcs_path = app_args.input_csv):
        self.client = GcsIO()
        self.path = gcs_path

    def process(self, tracker = beam.DoFn.RestrictionParam(PitchingCoach())):
        with self.client.open(self.path, mode = 'rb') as csv_file:
            start, stop = tracker.current_restriction()
            csv_file.seek(start)
            while tracker.try_claim(csv_file.tell()):
                next(DictReader(TextIOWrapper(
                    csv_file, newline = '', errors = 'replace'),
                    fieldnames = fieldnames))
                #     for row in csv_dict:
                #         yield row


fieldnames = [
    'id',
    'name',
    'host_id',
    'host_name',
    'neighbourhood_group',
    'neighbourhood',
    'latitude',
    'longitude',
    'room_type',
    'price',
    'minimum_nights',
    'number_of_reviews',
    'last_review',
    'reviews_per_month',
    'calculated_host_listings_count',
    'availability_365']

# "all tracker types have a tryClaim(P) operation: inside processElement(), 
# the SDF repeatedly consults the tracker using tryClaim(P) to claim a new block of work at position P


# class LoadCSV():
#     def __init__(self, gcs_path = app_args.input_csv):
#         self.client = GcsIO()
#         self.path = gcs_path
#     def process(self):
#         elements = []
#         with self.client.open(self.path, mode = 'rb') as csv_file:
#             csv_dict = DictReader(
#                 TextIOWrapper(csv_file, newline = '', errors = 'replace'))
#             for elem in csv_dict:
#                 elements.append(elem)
#         return elements


class FormatBNB(beam.DoFn):
    def process(self, element):

        # for easier indexing
        row = list(element.values())

        #remove newline characters in Name field, which describes a listing
        row[1] = row[1].replace(r'/n', r' ')

        # cast to int
        for i in [0,2,9,10,11,14,15]:
            row[i] = int(row[i])
        
        # cast to float; column 13, 'reviews_per_month', contains Nulls
        for i in [6,7,13]:
            row[i] = float(row[i]) if len(row[i]) > 0 else None
        
        # convert to BQ date; column 12, 'last_review', contains Nulls
        if len(row[12]) > 0:
            month,day,year = row[12].split('/')
            row[12] = year + '-' + month.zfill(2) + '-' + day.zfill(2)
        else:
            row[12] = None

        yield { k:v for k,v in zip(element.keys(), row) }


schema = [
    "id:INTEGER", 
    "name:STRING",
    "host_id:INTEGER",
    "host_name:STRING", 
    "neighbourhood_group:STRING",
    "neighbourhood:STRING",
    "latitude:FLOAT",
    "longitude:FLOAT",
    "room_type:STRING",
    "price:INTEGER",
    "minimum_nights:INTEGER",
    "number_of_reviews:INTEGER",
    "last_review:DATE",
    "reviews_per_month:FLOAT",
    "calculated_host_listings_count:INTEGER",
    "availability_365:INTEGER"]
schema = ",".join([f for f in schema])

#------------------MAIN LOGIC------------------------------------------

def run():

    #--------------SET OPTIONS-----------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-csv',
        # required = True,
        default = 'gs://airbnb-nyc-19/AB_NYC_2019.csv',
        help = "File path in Google Cloud Storage, 'gs://...'.")
    parser.add_argument(
        '--main-bq-table', default='bnb_19_sdf.bnb_table')
    parser.add_argument(
        '--counts-bq-table', default='bnb_19_sdf.nhood_counts')

    app_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        runner = 'DataflowRunner',
        project = 'practice-springml',
        region = 'us-central1',
        # job_name = 'practice-MP',
        temp_location = 'gs://practice-springml.appspot.com/tmp',
        # staging_location = 'gs://staging.practice-springml.appspot.com/stg',
        # template_location = 'gs://practice-job/template',
        save_main_session = True # so workers can access imported modules
        )

    #--------------STAGE PIPELINE--------------------------------------

    with beam.Pipeline(options=pipeline_options) as p:

        pcoll = (
            p 
            | 'sdf_read_csv' >> beam.ParDo(LoadCSV())
            | 'format_bnb' >> beam.ParDo(FormatBNB())
            )

        pcoll | 'write1' >> WriteToBigQuery(
            table = app_args.main_bq_table,
            schema = schema,
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE')

        (pcoll 
        | 'nhood_is_one' >> beam.Map(
            lambda row: (row['neighbourhood'], 1) )
        | 'nhood_counts' >> beam.CombinePerKey(sum)
        | 'format_tally' >> beam.Map(
            lambda cnt: {'neighbourhood': cnt[0], 'n_listings': cnt[1]} )
        | 'write2' >> WriteToBigQuery(
            table = app_args.counts_bq_table,
            schema = 'neighbourhood:STRING,n_listings:INTEGER',
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE')
            )
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()