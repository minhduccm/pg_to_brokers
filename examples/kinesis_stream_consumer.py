import threading
import time

from boto import kinesis


class Executor(threading.Thread):
    """docstring for Executor"""

    def __init__(self, client, stream_name, shard_id):
        super(Executor, self).__init__()
        self.client = client
        self.stream_name = stream_name
        self.shard_id = shard_id

    def process_records(self, record_response):
        print record_response

    def run(self):
        client = self.client
        shard_iterator = client.get_shard_iterator(
            stream_name=self.stream_name,
            shard_id=self.shard_id,
            shard_iterator_type='TRIM_HORIZON'
        )['ShardIterator']

        record_response = client.get_records(
            shard_iterator=shard_iterator,
            limit=1
        )
        self.process_records(record_response)
        while 'NextShardIterator' in record_response:
            record_response = client.get_records(
                shard_iterator=record_response['NextShardIterator'],
                limit=1
            )
            self.process_records(record_response)
            time.sleep(2)


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
    for shard in shards:
        executor = Executor(client, stream_name, shard['ShardId'])
        executor.start()


if __name__ == '__main__':
    main()
