import signal
import os
import threading
import time

from logger import Logger
from parser import Parser
from stream_reader import StreamReader
from stream_writer import StreamWriter
from util import auto_attr_check


class Processor(threading.Thread):
    """docstring for Processor"""

    def __init__(
        self,
        stream_processor
    ):
        super(Processor, self).__init__()
        self.stream_processor = stream_processor

    def clean_up(self, stream_processor):
        if stream_processor.detroy_slot_after_stopping is True:
            stream_processor.stream_reader \
                .destroy_logical_replication_slot_if_existed(
                    stream_processor.pg_cursor,
                    stream_processor.pg_connector,
                    stream_processor.logger
                )
        stream_processor.pg_cursor.close()
        stream_processor.pg_connector.close()

    def run(self):
        try:
            stream_processor = self.stream_processor
            logger = stream_processor.logger
            parser = stream_processor.parser
            delay_time = stream_processor.delay_time
            stream_reader = stream_processor.stream_reader
            stream_writer = stream_processor.stream_writer
            pg_cursor = stream_processor.pg_cursor
            stream_writer.init_broker_stuffs(logger)

            while (True):
                if stream_processor.is_stopped is True:
                    self.clean_up(stream_processor)
                    logger.info('Streaming process terminated...')
                    break
                logger.info('Start retrieving postgresql changes...')
                changes = stream_reader.retrieve_changes(pg_cursor)
                if len(changes) == 0:
                    logger.info('No changes...')
                    time.sleep(delay_time)
                    continue

                formatted_changes = parser.parse(changes, stream_reader.tables)
                if len(formatted_changes) == 0:
                    logger.info('There are few changes but not in \
                        supported operations (insert/update/delete)...')
                    time.sleep(delay_time)
                    stream_reader.delete_changes_after_comsumed(pg_cursor)
                    continue

                stream_writer.publish_changes_to_broker(
                    formatted_changes, logger)
                stream_reader.delete_changes_after_comsumed(pg_cursor)
                logger.info('Finished processing changes...')
                time.sleep(delay_time)

        except Exception as e:
            if stream_processor is not None and \
                    stream_processor.pg_connector is not None and \
                    stream_processor.pg_cursor is not None:
                self.clean_up(stream_processor)

            err_msg = 'Process terminated by getting error: {error}' \
                .format(error=e.message)
            print(err_msg)
            logger.error(err_msg)
            os._exit(1)


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
        is_stopped=False,
        detroy_slot_after_stopping=False
    ):
        super(StreamProcessor, self).__init__()
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.delay_time = delay_time
        self.log_info = log_info
        self.is_stopped = is_stopped
        self.detroy_slot_after_stopping = detroy_slot_after_stopping
        pg_connector, pg_cursor = stream_reader.init_pg_stuffs()
        self.pg_connector = pg_connector
        self.pg_cursor = pg_cursor
        self.logger = Logger(self.log_info).get_logger()
        self.parser = Parser()

    def register_exit_events(self):
        def event_handler(signum, frame):
            self.logger.info('Received exit signal...')
            self.stop()
        signal.signal(signal.SIGINT, event_handler)
        signal.signal(signal.SIGTERM, event_handler)

    def start(self):
        print('Started processing...')
        self.stream_reader.create_logical_replication_slot_if_not_existed(
            self.pg_cursor,
            self.pg_connector,
            self.logger
        )
        self.register_exit_events()
        processor = Processor(self)
        processor.start()
        self.logger.info('Started streaming process...')

    def stop(self):
        print('Stopping process...')
        # This is atomic operation (thread safe)
        self.is_stopped = True
