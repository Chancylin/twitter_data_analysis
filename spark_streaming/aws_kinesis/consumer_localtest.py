import boto3
import json
from datetime import datetime
import time
# from config import stream_name
stream_name = "twitter-data-kinesis"

# create kinesis client connection
session = boto3.Session(profile_name='chuangxin')
kinesis_client = session.client('kinesis', region_name='us-east-1')

# Describe the stream
response = kinesis_client.describe_stream(StreamName=stream_name)
print(response)

# Get all the shards
shards = response['StreamDescription']['Shards']
print(shards)

print(shards[0]["ShardId"])

# Get all the shards
def get_kinesis_shards(stream):
    """Return list of shard iterators, one for each shard of stream."""
    _shard_ids = [_shard["ShardId"] for _shard in shards]
    _shard_iters = [kinesis_client.get_shard_iterator(
        StreamName=stream,
        ShardId=_shard_id,
        ShardIteratorType="LATEST")
        for _shard_id in _shard_ids]
    return _shard_iters


shard_iters = get_kinesis_shards(stream_name)

################################
# Get stream data in batch
################################

# Iterate through all shard iterators in all shards and print 10 latest tweets
# Essentially tail -n10 for the Kinesis stream on each shard
for shard in shard_iters:
    records = kinesis_client.get_records(ShardIterator=shard["ShardIterator"],
                                         Limit=10)[u"Records"]
    for record in records:
        # print(record)
        # datum = json.loads(record[u"Data"])
        print(record[u"Data"])
        #print(json.dumps(datum, indent=4))
        # print(json.dumps(datum, indent=4, sort_keys=True))


###############################################
# Get stream data continuously from one shard
###############################################

shard_iter = shard_iters[0]

# get records from one shard first (it contains the `NextShardIterator`)
record_response = kinesis_client.get_records(ShardIterator=shard_iter["ShardIterator"], Limit=5)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=5)
    print(len(record_response['Records']), record_response)
    # wait for 5 seconds
    time.sleep(5)