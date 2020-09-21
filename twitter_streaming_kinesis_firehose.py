from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time

import twitter_credentials
from helper.extract_tweet_info import extract_tweet_info


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
        try:
            message = extract_tweet_info(tweet)
            # only put the record when message is not None
            if (message):
                firehose_client.put_record(
                    DeliveryStreamName=delivery_stream_name,
                    Record={
                        'Data': message
                    }
                )
        except (AttributeError, Exception) as e:
            print(e)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session(profile_name='chuangxin')

    # create the kinesis firehose client
    # firehose_client = <your code here>
    firehose_client = session.client('firehose', region_name='us-east-1')
    # firehose_client = session.client()

    # Set kinesis data stream name
    delivery_stream_name = "twitter-data"

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
