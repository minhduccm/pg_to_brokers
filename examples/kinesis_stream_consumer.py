import time

from boto import kinesis


def process_records(record_response):
    print record_response


def main():
    region = 'us-west-2'
    aws_access_key_id = 'aws_access_key_id'
    aws_secret_access_key = 'aws_secret_access_key'
    client = kinesis.connect_to_region(
        region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    stream_name = 'stream_name'
    stream_desciptor = client.describe_stream(stream_name=stream_name)
    shards = stream_desciptor['StreamDescription']['Shards']
    shard1 = shards[0]

    shard_iterator = client.get_shard_iterator(
        stream_name=stream_name,
        shard_id=shard1['ShardId'],
        shard_iterator_type='TRIM_HORIZON'
    )['ShardIterator']

    record_response = client.get_records(
        shard_iterator=shard_iterator,
        limit=1
    )
    process_records(record_response)
    while 'NextShardIterator' in record_response:
        record_response = client.get_records(
            shard_iterator=record_response['NextShardIterator'],
            limit=1
        )
        process_records(record_response)
        time.sleep(0.2)


if __name__ == '__main__':
    main()
