""" Natural Language Processing, esp sentiment, for Tweet series """

import pandas as pd
import en_core_web_sm
from spacytextblob.spacytextblob import SpacyTextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class SpacyTweets(object):
    """
    st = SpacyTweets(tweet_df.tweet)
    spacy_df = st.analyze()
    """
    def __init__(self, tweet_series):

        nlp = en_core_web_sm.load(
            exclude=[
                "parser", "attribute_ruler", "entity_linker",
                "tagger", "lemmatizer", "ner"])
        nlp.add_pipe('spacytextblob')
        self.doc_list = nlp.pipe(tweet_series)

    def analyze(self):

        polarity = []
        subjectivity = []
        word_sample = []
        tok2vec = []
        
        for doc in self.doc_list:
            polarity.append(doc._.polarity)
            subjectivity.append(doc._.subjectivity)
            word_sample.append([a[0][0] for a in doc._.assessments])
            tok2vec.append(doc.vector)

        spacy_df = pd.DataFrame(
            zip(polarity, subjectivity, word_sample, tok2vec), 
            columns=["polarity","subjectivity","word_sample","tok2vec"])

        return spacy_df


class VaderTweets(object):
    """
    vt = VaderTweets(tweet_df.tweet)
    vader_df = vt.analyze()
    """
    def __init__(self, tweet_series):

        self.vader = SentimentIntensityAnalyzer()
        self.tweets = tweet_series

    def analyze(self):

        v_dicts = self.tweets.apply(lambda x: self.vader.polarity_scores(x))

        vt_df = pd.DataFrame()

        for col in ['neg','neu','pos','compound']:
            vt_df[col] = v_dicts.apply(lambda x: x[col])

        return vt_df
