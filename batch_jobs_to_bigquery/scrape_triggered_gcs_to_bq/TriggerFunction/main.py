
def launch_batch_pipeline(data, context):

    from googleapiclient.discovery import build

    project = 'practice-springml-east'
    job = 'twitter_scrape_trigger_job' #  <- say it out loud
    template = 'gs://twitter-pipeline-template/dataflow-pipeline'

    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().templates().launch(
        projectId = project,
        gcsPath = template,
        # location = 'us-east1',
        body = {
            'jobName': job,
        }
    )

    response = request.execute()