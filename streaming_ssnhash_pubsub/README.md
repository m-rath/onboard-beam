

# messy code but functional pipeline based on [this](https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-dlp-hash-pipeline) 
# and [this](https://cloud.google.com/architecture/automating-classification-of-data-uploaded-to-cloud-storage)

___

main differences: 
- Cloud Function as initial trigger
- DLP job completes before Dataflow pipeline begins
- no Java and Makefile; just Python and Terraform

remaining doubts:
- original pipeline used Dataflow streaming pipeline
  - batch would suffice if few files with only marginal possibility of SSNs
  - Dataflow's ReadFromPubsub only works with streaming pipelines
  - to avoid Pubsub IO, different triggers or cron job could kick off pipeline, and DLP could happen inside pipeline
  - for now, the pipeline is streaming
- I wonder whether DLP could just use KMS and InfoType Custom Dictionary; then no need for dataflow; but since this exercise is practice, I will use Datastore, Dataflow, etc.
<br><br>

___

Cloud Function deployment:
```
gcloud functions deploy dlp_scan_gcs ^
--region us-east1 ^
--max-instances 2 ^
--trigger-resource ssnhash-demo3 ^
--trigger-event google.storage.object.finalize ^
--runtime python39 ^
--timeout 500s ^
--service-account ...@...iam.gserviceaccount.com
```

___
Flex Template deployment (from within `/dataflow_parts`):
```
SET REGION=us-east1
SET PROJECT_ID=practice-springml-ssnhash-py
SET TEMPLATE_IMAGE="gcr.io/%PROJECT_ID%/dataflow-streaming:latest"
SET BUCKET=%PROJECT_ID%-dataflow
SET TEMPLATE_PATH="gs://%BUCKET%/templates/pipeline.json"

gcloud builds submit --tag %TEMPLATE_IMAGE% .

gsutil mb gs://%BUCKET%

gcloud dataflow flex-template build %TEMPLATE_PATH% ^
    --image %TEMPLATE_IMAGE% ^
    --sdk-language "PYTHON" ^
    --metadata-file "pipeline.json"
```

___
Flex Template run:
```
gcloud dataflow flex-template run "streaming-beam-job" ^
    --template-file-gcs-location %TEMPLATE_PATH% ^
    --project %PROJECT_ID% ^
    --region %REGION% ^
```

Output:
```
job:
  createTime: '2022-03-06T01:43:19.227227Z'
  currentStateTime: '1970-01-01T00:00:00Z'
  id: 2022-03-05_17_43_18-12368386573144622713
  location: us-east1
  name: streaming-beam-job
  projectId: practice-springml-ssnhash-py
  startTime: '2022-03-06T01:43:19.227227Z'
```
___

### Take-aways: 
<br>
If this were not a practice exercise, I would forego Dataflow and possibly Firestore.
A single Cloud Function could cue DLP, hash every DLP finding, and query Firestore to confirm.
To forego Firestore, we could use a large custom dictionary in DLP to store hashed SSNs.
<br><br>
Eliminating Dataflow would allow us to eliminate PubSub until after DLP findings are hashed. An alternative way to eliminate PubSub early in the pipeline (pre-hashing) would be to trigger a Dataflow batch job from the Cloud Function with a Value Provider (classic template) accepting the
target GCS filename. Then Dataflow could call DLP, etc. An advantage here is that, without the need for a PubSub source, we could trigger Dataflow *batch* jobs, instead of the ill-suited streaming job required of ReadFromPubSub.
<br><br>
Anyway, this was a useful exercise -- a Cloud Function to DLP, a streaming pipeline with Firestore query and PubSub source and sink, and a Flex template for a change.

___

### Misc notes

output pubsub messages should probably include source file. <br>

where is salt stored? with hashed result, or...? os.urandom, then ...? <br>
ah, ok, it's just a uniform salt in this case; so our hashed SSNs will be different from the same SSNs hashed elsewhere. <br>

windowing, batching -- for bulk submission to firestore? or one-by-one? <br>
quotas treat batch of document transactions as separate transactions, regardless. <br>
firestore docs recommend streaming, if requests are in parallel <br>

NB: The gcp terraform deployment raised a couple questions answered [here](https://binx.io/blog/2020/07/06/enabling-firestore-from-terraform/). For example, why is App Engine required? (I ran into errors while trying to reproduce the gcp pro services' repo, and one required that I enable App Engine) <br><br>

MPNB: <br>
--staging-location "gs://ssnhash-temp" <-- error <br>
--staging-location "gs://ssnhash-temp/tmp" <-- ok <br>
"gs://ssnhash-temp" <-- bucket <br>
"gs://ssnhash-temp/tmp" <-- storage object <br>
