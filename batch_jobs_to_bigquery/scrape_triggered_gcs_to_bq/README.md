App Engine service queries Twitter at regular intervals, <br>
per QUERY variable in app.yaml and temporal specs in cron.yaml;<br><br>

then analyzes the tweets for sentiment,<br>
with minimal installations of spacytextblob and vaderSentiment;<br><br>

then exports csv file to default bucket, (gs://<project>.appspot.com), <br>
triggering a Cloud Function, which calls Dataflow to run template job. <br><br>

The Dataflow template will partition tweets according to sentiment --<br>
specifically vader compound score -- then ignore the neutral tweets<br>
while writing pos and neg tweets to their respective BigQuery sinks.<br><br>

Secret Manager holds the Twitter API keys (Elevated developer account),<br>
a Virtual Private Cloud insulates the project with firewall policies,<br>
and the App Admin API is enabled for patch updates of QUERY string.