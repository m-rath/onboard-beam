import mimetypes
import google.cloud.dlp
from google.cloud import pubsub
from google.cloud import storage


PROJECT_ID = "practice-springml-ssnhash-py"
BUCKET_TO_MONITOR = "ssnhash-demo3"
INFO_TYPES = ["US_SOCIAL_SECURITY_NUMBER"]
MIN_LIKELIHOOD = "POSSIBLE"
MAX_FINDINGS = 0 # no max
PUBSUB_TOPIC = "sensitive-probable"


def dlp_scan_gcs(event, context):

    """
    <https://cloud.google.com/dlp/docs/inspecting-text#inspecting_a_text_file>
    <https://cloud.google.com/storage/docs/json_api/v1/objects#resource>

    """

    #obviously there's a better way to pass variables; this is WIP
    project = PROJECT_ID
    bucket = event['bucket']
    filename = event['name']
    info_types = INFO_TYPES
    min_likelihood = MIN_LIKELIHOOD
    max_findings = MAX_FINDINGS
    include_quote = True
    mime_type = "text/plain"
    topic = f"projects/{project}/topics/{PUBSUB_TOPIC}"

    info_types = [{"name": info_type} for info_type in info_types]

    # Construct the configuration dictionary.
    inspect_config = {
        "info_types": info_types,
        "min_likelihood": min_likelihood,
        "include_quote": include_quote,
        "limits": {"max_findings_per_request": max_findings},
    }

    # If mime_type is not specified, guess it from the filename.
    if mime_type is None:
        mime_guess = mimetypes.MimeTypes().guess_type(filename)
        mime_type = mime_guess[0]

    # Select the content type index from the list of supported types.
    supported_content_types = {
        None: 0,  # "Unspecified"
        "image/jpeg": 1,
        "image/bmp": 2,
        "image/png": 3,
        "image/svg": 4,
        "text/plain": 5,
    }
    content_type_index = supported_content_types.get(mime_type, 0)

    # Construct the item, containing the file's byte data.
    client = storage.Client()
    gcs_bucket = client.bucket(bucket)
    gcs_file = gcs_bucket.get_blob(filename).download_as_bytes()
    item = {"byte_item": {"type_": content_type_index, "data": gcs_file}}


    # Convert the project id into a full resource id.
    parent = f"projects/{project}"

    # Call the API. Note this is different from create_dlp_job.
    dlp = google.cloud.dlp_v2.DlpServiceClient()
    response = dlp.inspect_content(
        request={
            "parent": parent, 
            "inspect_config": inspect_config, 
            "item": item}
    )

    # Publish the results to PubSub
    publisher = pubsub.PublisherClient()
    if response.result.findings:
        for finding in response.result.findings:
            try:
                publisher.publish(topic, bytes(finding.quote, 'utf-8'))
            except AttributeError:
                pass










