import time

from logger import Logger
from parser import Parser
from stream_reader import StreamReader
from stream_writer import StreamWriter
from util import auto_attr_check


@auto_attr_check
class StreamProcessor(object):

    # Regsiter type for attributes
    stream_writer = StreamWriter
    stream_reader = StreamReader

    def __init__(
        self,
        stream_reader,
        stream_writer,
        delay_time=0.1,
        log_info=None,
        is_stopped=False
    ):
        super(StreamProcessor, self).__init__()
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.delay_time = delay_time
        self.log_info = log_info
        self.is_stopped = is_stopped

    def start(self):
        logger = Logger(self.log_info).get_logger()
        parser = Parser()
        stream_reader = self.stream_reader
        stream_writer = self.stream_writer
        pg_connector, pg_cursor = stream_reader.init_pg_stuffs()
        stream_reader.create_logical_replication_slot_if_not_existed(
            pg_cursor,
            pg_connector,
            logger
        )
        while (True):
            if self.is_stopped is True:
                break
            logger.info('Start retrieving postgresql changes...')
            changes = stream_reader.retrieve_changes(pg_cursor)
            if len(changes) == 0:
                logger.info('No changes...')
                time.sleep(self.delay_time)
                # continue
                break

            formatted_changes = parser.parse(changes, stream_reader.tables)
            if len(formatted_changes) == 0:
                logger.info('There are few changes but not in \
                    one of supported operations (insert/update/delete)...')
                time.sleep(self.delay_time)
                stream_reader.delete_changes_after_comsumed(pg_cursor)
                # continue
                break

            stream_writer.init_broker_stuffs()
            stream_writer.publish_changes_to_broker(formatted_changes)
            stream_reader.delete_changes_after_comsumed(pg_cursor)
            logger.info('Finished to process changes...')
            time.sleep(self.delay_time)
            break

    # run on another thread to stop streaming process
    # or on consumer's kill signal listenner
    def stop(
        self,
        pg_cursor,
        pg_connector,
        logger,
        should_destroy_slot=True
    ):
        self.is_stopped = True
        if should_destroy_slot is True:
            self.stream_reader.destroy_logical_replication_slot_if_existed(
                pg_cursor,
                pg_connector,
                logger
            )

        # TODO: close cursors/connections
