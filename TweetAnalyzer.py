from textblob import TextBlob
import re
import pandas as pd

class TweetAnalyzer():
    """
    Functionality for analyzing tweets.
    """
    def clean_tweet(self, tweet):
        # remove @xxx
        # remove hashtags #xxx
        # remove special characters
        # remove hyperlinks
        return ' '.join(re.sub("([@#&][A-Za-z0-9_-]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ",
                               tweet).split())

    def analyze_sentiment(self, tweet):
        # a very simple sentiment analyses using textblob
        analysis = TextBlob(self.clean_tweet(tweet))

        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1

    def load_to_csv(self, fName):
        # load a structure file to pandas df
        # note the column names should be consistent with the file fields
        cols_name = ["tweet", "hashtags", "date", "retweet_count", "favorite_count",
                     "is_retweet", "truncated",
                     "user_id", "user_name", "user_screen_name", "user_followers_count",
                     "user_loc", "geo", "newline"]

        df = pd.read_csv(fName, names=cols_name, sep="\t", header=None)
        df.drop(columns=["newline"], inplace=True)

        return df
