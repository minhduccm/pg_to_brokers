import base64
import json

from boto import kinesis
from boto.kinesis.exceptions import ResourceNotFoundException
from stream_writer import StreamWriter


class KinesisWriter(StreamWriter):

    client = None
    stream_descriptor = None

    def __init__(
        self,
        region,
        stream_name,
        aws_access_key_id='',
        aws_secret_access_key='',
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

    def init_broker_stuffs(self, logger):
        def log_error_and_raise(err_msg, logger):
            print(err_msg)
            logger.error(err_msg)
            raise Exception(
                'Got error inside "init_broker_stuffs" function.')

        try:
            client = kinesis.connect_to_region(
                self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            ) if self.aws_access_key_id != '' \
                and self.aws_secret_access_key != '' else kinesis.connect_to_region(self.region)

            stream_descriptor = client.describe_stream(
                stream_name=self.stream_name
            )
            self.stream_descriptor = stream_descriptor
            self.client = client

        except ResourceNotFoundException:
            if self.stream_descriptor is None:
                err_msg = 'Could not find {stream_name} stream.' \
                    .format(stream_name=self.stream_name)
                log_error_and_raise(err_msg, logger)
                return
            status = self.stream_descriptor['StreamDescription']['StreamStatus']
            if 'ACTIVE' not in status:
                err_msg = 'Stream status: {status}. Should be ACTIVE.' \
                    .format(status=status)
                log_error_and_raise(err_msg, logger)

        except Exception as e:
            raise e

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
            records=records,
            stream_name=self.stream_name
        )

    def publish_changes_to_broker(self, formatted_changes, logger):
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
