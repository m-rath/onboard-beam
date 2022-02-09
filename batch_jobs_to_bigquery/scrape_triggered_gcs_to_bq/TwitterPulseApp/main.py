""" 
This app queries Twitter at regular intervals, 
per QUERY variable in app.yaml and temporal specs in cron.yaml;

then analyzes the tweets for sentiment,
with minimal installations of spacytextblob and vaderSentiment;

then exports csv file to default bucket, (gs://<project>.appspot.com), 
triggering a Cloud Function, which calls Dataflow to run template job. 

The Dataflow template will partition tweets according to sentiment --
specifically vader compound score -- then ignore the neutral tweets
while writing pos and neg tweets to their respective BigQuery sinks.

Secret Manager holds the Twitter API keys (Elevated developer account),
a Virtual Private Cloud insulates the project with firewall policies,
and the App Admin API is enabled for patch updates of QUERY string.
"""

import os
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request
from google.cloud import storage
from .twitter_client import TwitterConn
from .nlp_tools import VaderTweets, SpacyTweets


BUCKET = os.getenv("BUCKET")
QUERY = os.getenv("QUERY")

app = Flask(__name__)

@app.route('/', methods = ["GET", "POST"])
def root():

    conn = TwitterConn()
    storage_client = storage.Client()

    i = 0
    while i < 1:

        tweet_df = conn.search()

        tweet_df['tweet'] = tweet_df.tweet.apply(
            lambda x: x.replace('\n', ' '))

        # -----NLP stuff-----
        vt = VaderTweets(tweet_df.tweet)
        vader_df = vt.analyze()

        st = SpacyTweets(tweet_df.tweet)
        spacy_df = st.analyze()
        
        wide_df = pd.concat([tweet_df, vader_df, spacy_df], axis = 1)

        # -----Write to bucket-----
        bucket = storage_client.bucket(BUCKET)
        stamp = datetime.now().strftime("%Y-%m-%d %H%M%S")
        blob = bucket.blob('twitter_pulse ' + stamp + '.csv')
        blob.upload_from_string(wide_df.to_csv())

        time.sleep(600)
        i += 1

        # -----Extra step for display in browser----
        web_iter = zip(wide_df.compound, wide_df.tweet)


    return render_template(
        "base.html", title = "home", QUERY = QUERY, web_iter = web_iter)
