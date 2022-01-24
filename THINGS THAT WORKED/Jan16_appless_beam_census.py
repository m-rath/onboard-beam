
""" 
Mark Porath, Jan 16
"""

import argparse
import logging
import json
import requests
from copy import copy
from io import TextIOWrapper
from csv import DictReader
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsIter, AsDict, AsSingleton
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io import WriteToBigQuery, ReadFromText

from google.cloud import secretmanager 


#--------------SET OPTIONS-----------------------------------------
PROJECT_ID = "practice-springml"

parser = argparse.ArgumentParser()
parser.add_argument(
    '--input-csv', default = 'gs://airbnb-nyc-19/AB_NYC_2019.csv')
parser.add_argument('--main-bq-table', default = 'bnb_19.bnb_table')
parser.add_argument('--supp-bq-table', default = 'bnb_19.nhood_supp')

app_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(
    pipeline_args,
    runner = 'DataflowRunner',
    project = 'practice-springml',
    region = 'us-central1',
    temp_location = 'gs://practice-job/temp',
    requirements_file = "requirements.txt",
    save_main_session = True
    )

class LoadCSV():

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

class NeighbourhoodCounts(beam.PTransform):
    def expand(self, pcollection):
        counts_collection = (
            pcollection 
            | '(Nhood, 1)' >> beam.Map(lambda row: (row['neighbourhood'],1))
            | 'CombinePerKey' >> beam.CombinePerKey(sum)
            | 'Format Count' >> beam.Map(
                lambda cnt: {'neighbourhood': cnt[0], 'n_listings': cnt[1]})
        )
        return counts_collection
        

class AddPUMA(beam.DoFn):
    """
    input element: {'neighbourhood': 'Breezy Point', 'n_listings': 3}
    side input: {'Breezy Point': {
        'latitude': 40.56573, 
        'longitude': -73.8699,
        'puma_code': '04114',
        'nta_code': 'QN10',
        'nta_name': 'Breezy Point-Belle Harbor-Rockaway Park-Broad Channel'}}
    output: {
        'neighbourhood': 'Breezy Point', 
        'n_listings': 3, 
        'puma_code': '04114'}
    """
    def process(self, element, puma_lookup):

        nhood = element['neighbourhood'] 
        puma_code = puma_lookup[nhood]['puma_code']
        fin = {k:v for k,v in element.items()} 
        fin.update({'puma_code': puma_code})
        # {'neighbourhood': 'Rossville', 'n_listings': 1, 'puma_code': '03901'}
        # {'neighbourhood': 'Breezy Point', 'n_listings': 3, 'puma_code': '04114'}
        # {'neighbourhood': 'Willowbrook', 'n_listings': 1, 'puma_code': nan}
        yield fin

class CensusRequest(beam.DoFn):

    def process(self, element, api_key): # side inputs
        # element = {'neighbourhood': 'Rossville', 'n_listings': 1, 'puma_code': '03901'}
        # element = {'neighbourhood': 'Breezy Point', 'n_listings': 3, 'puma_code': '04114'}
        # element = {'neighbourhood': 'Willowbrook', 'n_listings': 1, 'puma_code': nan}
        if element and type(element['puma_code']) == str: 

            code = element['puma_code']
            url1 = r"https://api.census.gov/data/2019/acs/acs1?get=NAME,"
            vs = "B01001_001E,B25077_001E,B29002_001E,B29002_007E,B25031_001E"
            url2 = rf"&for=public+use+microdata+area:{code}"
            url3 = rf"&in=state:36&key={CENSUS_API_KEY}"
            response = requests.get(url1 + vs + url2 + url3)
            arr = json.loads(response.content)
            dt = {k:v for k,v in zip(arr[0], arr[1])}

            fin = {k:v for k,v in element.items()} 
            fin.update({'median_rent': dt['B25031_001E']})
            fin.update({'median_home_value': dt['B25077_001E']})
            fin.update({'percent_adults_bachdeg': round(
                100*int(dt['B29002_007E'])/int(dt['B29002_001E']),2)})
            fin.update({'population': dt['B01001_001E']})
            fin.update({'puma': dt['NAME']})
            yield fin
        else:
            fin = {k:v for k,v in element.items()} 
            fin['puma_code'] = ''
            yield fin


class SuppDemographics(beam.PTransform):
    def __init__(self, puma_lookup, api_key):
        self.puma_lookup = puma_lookup
        self.api_key = api_key
    def expand(self, pcollection):
        supped_coll = (
            pcollection 
            | "Lookup PUMA Code" >> beam.ParDo(
                AddPUMA(), AsSingleton(self.puma_lookup))
            | "API Demographics" >> beam.ParDo(
                CensusRequest(), self.api_key)
        )
        return supped_coll

def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

CENSUS_API_KEY = access_secret_version(PROJECT_ID, 'CENSUS_API_KEY', 'latest')

with open('nhood_lookup.json', 'r') as json_file:
    puma_lookup = json.load(json_file)

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
    "puma_code:STRING",
    "median_rent:INTEGER",
    "median_home_value:INTEGER",
    "percent_adults_bachdeg:FLOAT",
    "population:INTEGER",
    "puma:STRING"]
schema_supp = ",".join([field for field in schema_supp])

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

        puma_js = pipe | beam.Create(puma_lookup) | beam.transforms.combiners.ToDict()

        (orig_rows 
            | 'Combine Count' >> NeighbourhoodCounts()
            | 'Request Census' >> SuppDemographics(puma_js, CENSUS_API_KEY)
            | 'Write to BQ' >> WriteToBigQuery(
                table = app_args.supp_bq_table,
                schema = schema_supp,
                create_disposition = 'CREATE_IF_NEEDED',
                write_disposition = 'WRITE_TRUNCATE')
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()