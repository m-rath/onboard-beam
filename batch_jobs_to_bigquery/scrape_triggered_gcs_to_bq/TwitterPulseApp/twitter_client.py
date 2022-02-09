""" Establish connection with Twitter API """

import os
import pandas as pd
from google.cloud import secretmanager
import tweepy


QUERY = os.getenv("QUERY")
PROJECT_ID = os.getenv("PROJECT_ID")

# Twitter API keys and tokens from GCP Secret Manager
def retrieve_secret(client, project_id, variable):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = client.secret_path(project_id, variable)
    version_path = client.get_secret_version(
        secret_path + r'/versions/latest')
    value = client.access_secret_version(
        version_path.name).payload.data.decode("UTF-8")
    return value

client = secretmanager.SecretManagerServiceClient()

KEY = retrieve_secret(client, PROJECT_ID, "TWITTER_API_KEY")
KEY_SECRET = retrieve_secret(client, PROJECT_ID, "TWITTER_API_KEY_SECRET")
TOKEN = retrieve_secret(client, PROJECT_ID, "TWITTER_ACCESS_TOKEN")
TOKEN_SECRET = retrieve_secret(
    client, PROJECT_ID, "TWITTER_ACCESS_TOKEN_SECRET")
BEARER_TOKEN = retrieve_secret(client, PROJECT_ID, "TWITTER_BEARER_TOKEN")


class TwitterConn(object):

    def __init__(self):

        self.client = tweepy.Client(
            bearer_token = BEARER_TOKEN, 
            consumer_key = KEY, 
            consumer_secret = KEY_SECRET, 
            access_token = TOKEN, 
            access_token_secret = TOKEN_SECRET,
            wait_on_rate_limit = False)

    def search(self, query = QUERY, max_results = 100):
        """
        QUERY = "tesla OR TSLA"
        QUERY is set in app.yaml, mutable via patch with App Admin API
        """
        response = self.client.search_recent_tweets(
            query = query, 
            expansions = "author_id", 
            max_results = max_results)

        response_size = response.meta['result_count']
        # users = response.includes['users']
        tweets = response.data

        rows = []
        for i in range(response_size):
            row = {
                # 'user_id' : users[i]['id'],
                # 'username' : users[i]['username'],
                # 'name' : users[i]['name'],
                'tweet_id' : tweets[i]['id'],
                'tweet' : tweets[i]['text']
            }
            rows.append(row)

        return pd.DataFrame(rows)