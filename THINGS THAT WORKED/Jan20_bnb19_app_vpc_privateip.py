
""" 
Mark Porath, Jan 20
"""

import argparse
import logging
import json
import requests
from io import TextIOWrapper
from csv import DictReader
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io import WriteToBigQuery


PROJECT_ID = "practice-springml"
API_URL = "https://practice-springml.uc.r.appspot.com"


parser = argparse.ArgumentParser()
parser.add_argument(
    '--input-csv', default = 'gs://airbnb-nyc-19/AB_NYC_2019.csv')
parser.add_argument('--main-bq-table', default = 'bnb_19.bnb_table')
parser.add_argument('--supp-bq-table', default = 'bnb_19.listings_by_nta')

app_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    runner = 'DataflowRunner',
    project = 'practice-springml',
    region = 'us-central1',
    temp_location = 'gs://practice-job/temp',
    subnetwork = 'regions/us-central1/subnetworks/bnb19-supp-load',
    use_public_ips = False,
    save_main_session = True
    )


schema_main = [
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
schema_main = ",".join([field for field in schema_main])

schema_supp = [
    "neighbourhood:STRING",
    "n_listings:INTEGER",
    "nta_code:STRING",
    "median_rent:INTEGER",
    "median_home_value:INTEGER",
    "percent_atleast_bachdeg:FLOAT",
    "population:INTEGER",
    "nta_name:STRING"]
schema_supp = ",".join([field for field in schema_supp])


class LoadCSV():
    """
    NB: not a DoFn; csv module is convenient, and this dataset is small
    """

    def __init__(self, gcs_path = app_args.input_csv):
        self.client = GcsIO()
        self.path = gcs_path

    def process(self):
        elements = []
        with self.client.open(self.path, mode = 'rb') as csv_file:
            csv_dict = DictReader(
                TextIOWrapper(csv_file, newline = '', errors = 'replace'))
            for elem in csv_dict:
                elements.append(elem)
        return elements


class FormatBNB(beam.DoFn):
    def process(self, element):

        # for easier indexing here
        row = list(element.values())

        # remove newline characters in Name field, i.e. bnb description
        row[1] = row[1].replace('/n', ' ')

        # cast to int
        for i in [0, 2, 9, 10, 11, 14, 15]:
            row[i] = int(row[i])
        
        # cast to float; column 13, 'reviews_per_month', contains Nulls
        for i in [6, 7, 13]:
            row[i] = float(row[i]) if len(row[i]) > 0 else None
        
        # convert to BQ date; column 12, 'last_review', contains Nulls
        if len(row[12]) > 0:
            month,day,year = row[12].split('/')
            row[12] = year + '-' + month.zfill(2) + '-' + day.zfill(2)
        else:
            row[12] = None

        yield { k:v for k,v in zip(element.keys(), row) }


class NeighbourhoodCounts(beam.PTransform):

    def expand(self, pcollection):

        counts_collection = (
            pcollection 
            | '(Nhood, 1)' >> beam.Map(lambda row: (row['neighbourhood'], 1))
            | 'CombinePerKey' >> beam.CombinePerKey(sum)
            | 'Format Count' >> beam.Map(
                lambda cnt: {'neighbourhood': cnt[0], 'n_listings': cnt[1]})
        )

        return counts_collection
        
def format_result(element, stats):
    """
    cast to integers; reorder items to match final schema
    """

    fin = {k:v for k,v in element.items()}
    


    for stat in ['median_home_value', 'median_rent', 'population']:
        if type(stats[stat]) == str:
            stats[stat] = int( stats[stat].replace(",", "") )
        else:
            # Stuyvesant median_home_value is NaN
            # WriteToBigQuery dislikes NaN 
            stats[stat] = ""

    fin.update({'nta_code': stats['nta_code']})
    fin.update({'median_rent': stats['median_rent']})
    fin.update({'median_home_value': stats['median_home_value']})
    fin.update({'percent_atleast_bachdeg': stats['percent_atleast_bachdeg']})
    fin.update({'population': stats['population']})
    fin.update({'nta_name': stats['nta_name']})

    return fin

class RequestDemographics(beam.DoFn):
    """
    input element: {'neighbourhood': 'Harlem', 'n_listings': 2658}

    output : {'neighbourhood': 'Harlem',
                'n_listings': 2658,
                'nta_code': 'MN1002',
                'median_rent': 1089,
                'median_home_value': 609513,
                'percent_atleast_bachdeg': 35.1,
                'population': 82712,
                'nta_name': 'Harlem (North)'}
    """
    def process(self, element, api_url):

        nhood = element['neighbourhood']
        
        response = requests.get(api_url, params={"neighbourhood": f"{nhood}"})
        
        demographics = json.loads(response.text)

        fin = format_result(element, demographics)

        yield fin


#------------------MAIN LOGIC------------------------------------------

def run():

    loader = LoadCSV()
    csv_dicts = loader.process()

    with beam.Pipeline(options=pipeline_options) as pipe:

        orig_rows = (
            pipe
            | 'CSV Reader' >> beam.Create(csv_dicts)
            | 'Clean Read' >> beam.ParDo(FormatBNB())
            )

        (orig_rows 
            | 'Load to BQ' >> WriteToBigQuery(
                table = app_args.main_bq_table,
                schema = schema_main,
                create_disposition = 'CREATE_IF_NEEDED',
                write_disposition = 'WRITE_TRUNCATE')
            )

        (orig_rows 
            | 'Combine Count' >> NeighbourhoodCounts()
            | 'Request App Engine' >> beam.ParDo(RequestDemographics(), API_URL)
            | 'Write to BQ' >> WriteToBigQuery(
                table = app_args.supp_bq_table,
                schema = schema_supp,
                create_disposition = 'CREATE_IF_NEEDED',
                write_disposition = 'WRITE_TRUNCATE')
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()