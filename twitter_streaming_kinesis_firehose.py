from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time


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
            if 'text' in tweet.keys():
                # Customize the message you want to send to the firehose
                # Parse the twitter record/json
                # Extract the columns you need

                # need to take care of truncated texts longer than 140 characters. get full text
                # full_text =

                message_lst = [str(tweet['id']),
                       str(tweet['user']['name']),
                       str(tweet['user']['screen_name']),
                       tweet['text'].replace('\n',' ').replace('\r',' '),
                       str(tweet['user']['followers_count']),
                       str(tweet['user']['location']),
                       str(tweet['geo']),
                       str(tweet['created_at']),
                       '\n'
                       ]

                # message = <your code here>

                message = "\t".join(message_lst)
                # print(message)

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
    delivery_stream_name = "twitter-week2"

    # Set twitter credentials

    # this is your own twitter credential
    consumer_key = 'V'
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            print('Twitter streaming...')

            # create instance of the tweet stream listener
            myStreamlistener = TweetStreamListener()

            # create instance of the tweepy stream
            stream = Stream(auth = auth, listener = myStreamlistener)

            # search twitter for the keyword
            stream.filter(track=["#AI", "#MachineLearning"], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
