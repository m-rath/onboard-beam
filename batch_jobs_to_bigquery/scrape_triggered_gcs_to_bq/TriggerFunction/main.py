import re
from google.cloud import storage
from googleapiclient.discovery import build


def launch_batch_pipeline(data, context):
    
    """
    The file whose arrival in GCS triggers this function is 
    immmediately processed by Dataflow and lands in BigQuery; 
    a Classic Template accepts the file path via Beam's ValueProvider.
    """

    client = storage.Client()
    bucket = client.get_bucket("practice-springml-east.appspot.com")
    obj = list(bucket.list_blobs())[-1]
    file_path = re.findall(".*csv", obj.id)[0]
    full_path = r"gs://" + file_path

    project = 'practice-springml-east'
    job = 'twitter_scrape_trigger_job' #  <- say it out loud
    template = 'gs://twitter-pipeline-template/dataflow-pipeline'

    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().create(
        projectId = project,
        location = 'us-east1',
        body = {
            'jobName': job,
            'parameters': {
                'input': full_path
                },
            'gcsPath': template
        })

    response = request.execute()