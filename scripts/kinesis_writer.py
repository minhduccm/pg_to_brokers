import base64
import boto3
import json

from stream_writer import StreamWriter


class KinesisWriter(StreamWriter):

    client = None

    def __init__(
        self,
        region,
        aws_access_key_id,
        aws_secret_access_key,
        stream_name,
        number_of_records_to_send=5,
        default_partition_key='Default'
    ):
        super(KinesisWriter, self).__init__()
        self.number_of_records_to_send = number_of_records_to_send
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.stream_name = stream_name
        self.default_partition_key = default_partition_key

    def init_broker_stuffs(self):
        client = boto3.client(
            'kinesis',
            self.region
            # TODO: uncomment belows after testing
            # aws_access_key_id=self.aws_access_key_id,
            # aws_secret_access_key=self.aws_secret_access_key
        )
        self.client = client

    def assign_change_to_partition_key(self, change):
        return self.default_partition_key

    def standardise_kinesis_format(self, changes):
        records = []
        for change in changes:
            records.append(
                {
                    'Data': base64.b64encode(
                        json.dumps(change).encode('utf-8')
                    ),
                    'PartitionKey': self.assign_change_to_partition_key(change)
                }
            )
        return records

    def put_records_chunk(self, chunk):
        client = self.client
        records = self.standardise_kinesis_format(chunk)
        client.put_records(
            Records=records,
            StreamName=self.stream_name
        )

    def publish_changes_to_broker(self, formatted_changes):
        offset = self.number_of_records_to_send
        len_changes = len(formatted_changes)
        skip = 0
        i = 0
        while True:
            skip = i * offset
            if skip + offset >= len_changes:
                chunk = formatted_changes[skip:len_changes]
                self.put_records_chunk(chunk)
                break

            chunk = formatted_changes[skip:skip + offset]
            self.put_records_chunk(chunk)
            i = i + 1
