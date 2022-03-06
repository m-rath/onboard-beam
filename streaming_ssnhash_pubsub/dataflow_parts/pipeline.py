

import re
import hmac
import logging
import argparse
from typing import List
from base64 import b64decode
from google.cloud import secretmanager, firestore
import apache_beam as beam
from apache_beam.pipeline import Pipeline, PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub

    
def access_hashkey(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data
    return b64decode(payload)


class HashSSN(beam.DoFn):

    def __init__(self, salt):
        self.salt = salt.encode('utf-8')
    
    def process(self, elem, hashkey):
        unverified_ssn = elem.strip().replace('-', '')
        if not re.match(r'[0-9]{9}', unverified_ssn):
            raise ValueError(f"{unverified_ssn} is not a 9 digit number")
        mac = hmac.new(hashkey, msg = self.salt, digestmod = 'sha256')
        mac.update(unverified_ssn.encode('utf-8'))
        yield mac.hexdigest()


class QueryFirestore(beam.DoFn):

    def setup(self):
        self.db = firestore.Client()

    def process(self, element, db_collection):
        doc_ref = self.db.document(db_collection, element)
        doc = doc_ref.get()
        if doc.exists:
            yield element


def run(
    project_id: str,
    input_sub: str,
    output_topic: str,
    secret: str,
    salt: str,
    collection: str,
    # beam_args: List[str] = None
    ) -> None:
    
    options = PipelineOptions(
        # beam_args,
        project = project_id,
        save_main_session = True,
        streaming = True)

    hashkey = access_hashkey(project_id, secret, "latest")

    with Pipeline(options = options) as p:

        (p 
        | ReadFromPubSub(
            subscription = f"projects/{project_id}/subscriptions/{input_sub}")
        | beam.WindowInto(beam.window.FixedWindows(30))
        | beam.Map(lambda x: x.decode('utf-8'))
        | beam.ParDo(HashSSN(salt), hashkey)
        | beam.ParDo(QueryFirestore(), collection)
        | beam.Map(lambda x: x.encode('utf-8'))
        | WriteToPubSub(topic = f"projects/{project_id}/topics/{output_topic}")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        default = "practice-springml-ssnhash-py",
        type = str
    )
    parser.add_argument(
        "--input_sub",
        default = "dlp_hit-check_datastore",
        type = str
    )
    parser.add_argument(
        "--output_topic",
        default = "sensitive-confirmed",
        type = str
    )
    parser.add_argument(
        "--secret",
        default = "hash-key-64",
        type = str
    )
    parser.add_argument(
        "--salt",
        default = "eeba2d314a46c84ed4c3cab991d0359a",
        type = str
    )
    parser.add_argument(
        "--collection",
        default = "hashed_socials",
        type = str
    )

    args, beam_args = parser.parse_known_args()

    run(
        project_id = args.project, # parse_known_args infers from prefixes, so beam_args does not collect "project" from gcloud command flag  
        input_sub = args.input_sub,
        output_topic = args.output_topic,
        secret = args.secret,
        salt = args.salt,
        collection = args.collection,
        # beam_args = beam_args, # should collect region, staging_location, temp_location
    )