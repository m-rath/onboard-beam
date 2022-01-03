"""
Mark Porath

<https://beam.apache.org/releases/pydoc/2.34.0/apache_beam.transforms.core.html#apache_beam.transforms.core.ParDo.with_exception_handling>
<https://beam.apache.org/documentation/patterns/bigqueryio/#google-bigquery-patterns>

<https://www.youtube.com/watch?v=j5qYzjyGrCs>
<https://beam.apache.org/releases/pydoc/2.34.0/apache_beam.transforms.core.html#apache_beam.transforms.core.ParDo.with_outputs>

the result is ok, but not great -- see description in cloud console;
the python library 're' is useful, but beam.io.ReadFromText is too limited;
beam.dataframe module is supposed to make csv sources easy, but I found a bug:
to_pcollection fails when dataframes have nulls -- fix is weeks away, beam 2.36

practice-springml:nyc19_dataset_25.bnb_table
practice-springml:nyc19_dataset_25.listings_per_hood
"""

import re, logging
import apache_beam as beam
# from apache_beam.dataframe.io import read_csv
# from apache_beam.dataframe.convert import to_pcollection
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToBigQuery


options = PipelineOptions([
    '--runner', 'DataflowRunner', 
    '--project', 'practice-springml', 
    # '--template_location', 'gs://practice-job/template',
    '--temp_location', 'gs://practice-springml.appspot.com/tmp',    
    '--region', 'us-central1',
    '--save_main_session'
    ])


def format_bnb(row):

    fields = [
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
        'availability_365'
        ]

    p = re.compile(r"""(?xs:(\d+),(.*),(\d{4,9}),(.*),
                        ([BMQS].*),  # the 5 boroughs
                        ([A-Z]\D*),  # neighbourhood starts with capital
                        (40\.\d+),   # latitude (40.49979, 40.91306)
                        (-\d+\.\d+), # longitude (-74.24442, -73.71299)
                        (.*),(\d+),(\d+),(\d+),
                        (.*),(.*),   # last_review, reviews_per_month
                        (\d+),(\d+))""")
                        
    m = p.findall(row)

    # COULD PRACTICE DEADLETTER PATTERN HERE (not implemented yet);
    # ERRORS HAVE NEWLINE CHARACTER IN 'NAME'
    if len(m) == 0:
        nulls = [None] * 16
        return { k:v for k,v in zip(fields, nulls) } #yield?

    # cast to int
    m = list(m[0])
    for i in [0,2,9,10,11,14,15]:
        m[i] = int(m[i])
    
    # cast to float; column 13, 'reviews_per_month', contains Nulls
    for i in [6,7,13]:
        if len(m[i]) > 0:
            m[i] = float(m[i])
        else:
            m[i] = None
    
    # convert to BQ date; column 12, 'last_review', contains Nuls
    if len(m[12]) > 0:
        month,day,year = m[12].split('/')
        m[12] = year+'-'+month.zfill(2)+'-'+day.zfill(2)
    else:
        m[12] = None

    return { k:v for k,v in zip(fields, m) }


def tally_nhood(listing):
    if listing:
        return (listing['neighbourhood'], 1)


def format_tally(nhood_count):
    return {'neighbourhood':nhood_count[0], 'n_listings':nhood_count[1]}


def run(options = options):

    schema0 = [
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
    schema1 = ",".join([f for f in schema0])

    gcs_csv = "gs://airbnb-nyc-19/AB_NYC_2019.csv"


    

    p = beam.Pipeline(options = options)    

        #----------EXTRACT---------------------------------------------
        # parsed_records, parsing_errors = p | "Extract and Parse" >> ExtractDataTransform(app.args.input_csv)

        #----------TRANSFORM-------------------------------------------
        # results = parsed_records | "Clean and Calculate" >> PreprocessingTransform(...)

        #----------LOAD------------------------------------------------
    pcoll = (
        p 
        | 'read_csv' >> ReadFromText(gcs_csv, skip_header_lines=1)
        | 'format_bnb' >> beam.Map(format_bnb)
        )

    (pcoll 
    | 'write1' >> WriteToBigQuery(
        table = "nyc19_dataset_25.bnb_table",
        schema = schema1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE'))

    (pcoll 
    | 'nhood_is_one' >> beam.Map(tally_nhood) 
    | 'nhood_counts' >> beam.CombinePerKey(sum)
    | 'format_tally' >> beam.Map(format_tally) 
    | 'write2' >> WriteToBigQuery(
        table = "nyc19_dataset_25.listings_per_nhood",
        schema = 'neighbourhood:STRING,n_listings:INTEGER',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE'))

    p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()