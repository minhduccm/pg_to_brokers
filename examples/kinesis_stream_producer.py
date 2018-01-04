from scripts.kinesis_writer import KinesisWriter
from scripts.stream_processor import StreamProcessor
from scripts.stream_reader import StreamReader


def main():
    # stream reader
    pg_info = {
        'host': 'host_name',
        'port': 5432,
        'database': 'db_name',
        'user': 'user',
        'password': 'password'
    }
    slot_name = 'pg_slot'
    upto_nchanges = 1
    tables = []
    stream_reader = StreamReader(
        pg_info=pg_info,
        slot_name=slot_name,
        upto_nchanges=upto_nchanges,
        tables=tables
    )

    # stream writer
    broker_info = {}
    stream_writer = KinesisWriter(broker_info=broker_info)

    # stream processor
    log_info = {
        'log_path': '.',
        'log_file_name': 'my_log_file.log'
    }
    stream_processor = StreamProcessor(
        stream_reader=stream_reader,
        stream_writer=stream_writer,
        log_info=log_info
    )

    stream_processor.start()


if __name__ == '__main__':
    main()
