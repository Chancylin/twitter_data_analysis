from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time

import twitter_credentials
from helper.extract_tweet_info import extract_tweet_info
from config import stream_name


class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        """Summary

        Args:
            data (TYPE): Description

        Returns:
            TYPE: Description
        """
        tweet = json.loads(data)
        # TODO: collect a certain amount of record before put it. Though this may be
        # impossible due to the way that the StreamListener receives data
        try:
            payload = extract_tweet_info(tweet)
            # only put the record when message is not None
            if (payload):
                # print(payload)
                # note that payload is a list
                put_response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=str(tweet['user']['screen_name'])
                )

            return True
        except (AttributeError, Exception) as e:
            print(e)


    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session(profile_name='chuangxin')

    # create the kinesis client
    kinesis_client = session.client('kinesis', region_name='us-east-1')

    # set twitter keys/tokens
    auth = OAuthHandler(twitter_credentials.consumer_key, twitter_credentials.consumer_secret)
    auth.set_access_token(twitter_credentials.access_token, twitter_credentials.access_token_secret)

    while True:
        try:
            print('Twitter streaming...')

            # create instance of the tweet stream listener
            myStreamlistener = TweetStreamListener()

            # create instance of the tweepy stream
            stream = Stream(auth=auth, listener=myStreamlistener)

            # search twitter for the keyword
            stream.filter(track=["#AI", "#MachineLearning"], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
