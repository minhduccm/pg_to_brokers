import time

from pg_to_brokers.kinesis_writer import KinesisWriter
from pg_to_brokers.stream_processor import StreamProcessor
from pg_to_brokers.stream_reader import StreamReader


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
        # [REQUIRED]
        # postgresql information to connect to
        pg_info=pg_info,

        # [OPTIONAL]
        # slot name, default: pg_slot
        slot_name='pg_slot',

        # [OPTIONAL]
        # number of changes you'd like to retrieve at once
        upto_nchanges=1,

        # [OPTIONAL]
        # table names you'd like to stream changes from
        # if not set or empty then getting all tables
        tables=[]
    )

    # stream writer
    # NOTE:
    # - aws_access_key_id & aws_secret_access_key are optional params, you can specify them with yours.
    # - I'm using boto library to connect to AWS Kinesis, so you can configure pair of key above with boto's configuration.
    # - Alternatively, you can also configure "Role" with permission to talk to your Kinesis (if you're running the script on EC2 instance)
    # - With both ways above, you'll not need to specify keys here.
    stream_writer = KinesisWriter(
        # [REQUIRED]
        region='aws_region',  # eg. 'us-west-2'

        # [REQUIRED]
        stream_name='stream_name',

        # [OPTIONAL]
        aws_access_key_id='aws_access_key_id',

        # [OPTIONAL]
        aws_secret_access_key='aws_secret_access_key',

        # [OPTIONAL]
        # Number of records to send to kinesis at once
        # Default: 5
        number_of_records_to_send=5,

        # [OPTIONAL]
        # Default partition key
        # Default: Default
        default_partition_key='Default'
    )

    # stream processor
    log_info = {
        'log_path': 'log_path',
        'log_file_name': 'my_log_file.log'
    }
    stream_processor = StreamProcessor(
        # [REQUIRED]
        stream_reader=stream_reader,

        # [REQUIRED]
        # stream_writer attr MUST be an instance of StreamWriter's subclass
        stream_writer=stream_writer,

        # [OPTIONAL]
        # log_info specifies where you'd like to place your logs
        log_info=log_info,

        # [OPTIONAL]
        # whether you'd like to destroy replication slot from PostgreSQL or not
        destroy_slot_after_stopping=False,

        # [OPTIONAL]
        # delay time in seconds between 2 iteration of process,
        # it helps "refresh" CPU
        # default: 0.1
        delay_time=0.5
    )

    stream_processor.start()
    time.sleep(7)
    # instead of sleep 7s,
    # you can do some logics of conditions
    # to terminate streaming process gracefully
    stream_processor.stop()


if __name__ == '__main__':
    main()
