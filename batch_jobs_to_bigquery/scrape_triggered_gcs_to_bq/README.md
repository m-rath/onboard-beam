The App Engine service 
1. queries Twitter at regular intervals, per QUERY variable in app.yaml and temporal specs in cron.yaml;<br>
2. analyzes the tweets for sentiment, with minimal installations of spacytextblob and vaderSentiment;<br>
3. exports csv file to GCS, triggering a Cloud Function, which launches a Dataflow template. <br><br>

The Cloud Function deployment:
- gcloud functions deploy launch_batch_pipeline<br> --region us-east1<br> --max-instances 2<br> --trigger-resource practice-springml-east.appspot.com<br> --trigger-event google.storage.object.finalize<br> --runtime python39<br> --timeout 500s<br> --service-account practice-beam@practice-springml-east.iam.gserviceaccount.com<br><br>

The Dataflow batch pipeline partitions the tweet set according to sentiment -- specifically, by vader compound score -- ignoring neutral tweets while writing pos and neg tweets to their respective BigQuery sinks.
- NB: the Dataflow template is Classic, not Flex; the only way to vary input source to a Classic template is via ValueProvider.<br><br>

Other notes:
- The GCS bucket where these twitter scrapes land has a Lifecycle policy: files are deleted after a day.
- The cron job runs every 11 hours and deposits fewer than 100 tweets into each table per job.
- Secret Manager holds the Twitter credentials; the elevated developer account is permitted to scrape as many as 200 tweets every 4 minutes.
- The App Admin API is enabled on this project, so QUERY variable in app.yaml could be revised without re-deploying app service, I believe.