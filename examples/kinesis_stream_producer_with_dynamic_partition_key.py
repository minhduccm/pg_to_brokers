import time

from scripts.kinesis_writer import KinesisWriter
from scripts.stream_processor import StreamProcessor
from scripts.stream_reader import StreamReader


class KinesisWriterWithDynamicPartitionKey(KinesisWriter):
    """docstring for KinesisWriterWithDynamicPartitionKey"""

    def __build_change_obj(self, change):
        obj = {}
        for idx, field in change['fields']:
            obj[field] = change['values'][idx]
        return obj

    def assign_change_to_partition_key(self, change):
        stream_descriptor = self.stream_descriptor
        shard_len = len(stream_descriptor['StreamDescription']['Shards'])
        change_obj = self.__build_change_obj(change)
        partition_key = change_obj['id'] % shard_len
        # NOTE: ex - if type of field is not a number
        # then we can user a hash function to map that field to number
        # prior to doing modulo
        return partition_key


def main():
    # stream reader
    pg_info = {
        'host': 'host',
        'port': 5432,
        'database': 'database',
        'user': 'user',
        'password': 'password'
    }
    stream_reader = StreamReader(
        pg_info=pg_info,
        slot_name='pg_slot',
        upto_nchanges=1,
        tables=[]
    )

    # stream writer
    stream_writer = KinesisWriterWithDynamicPartitionKey(
        region='us-west-2',  # eg. 'us-west-2'
        aws_access_key_id='aws_access_key_id',
        aws_secret_access_key='aws_secret_access_key',
        stream_name='stream_name',
        number_of_records_to_send=5,  # number of records to send to broker once
        default_partition_key='Default'
    )

    # stream processor
    log_info = {
        'log_path': 'log_path',
        'log_file_name': 'my_log_file.log'
    }
    stream_processor = StreamProcessor(
        stream_reader=stream_reader,
        # stream_writer attr MUST be an instance of StreamWriter's subclass
        stream_writer=stream_writer,
        log_info=log_info,
        detroy_slot_after_stopping=False,
        delay_time=0.5
    )

    stream_processor.start()
    time.sleep(20)
    # instead of sleep 7s,
    # you can do some logics of conditions
    # to terminate streaming process gracefully
    stream_processor.stop()


if __name__ == '__main__':
    main()
