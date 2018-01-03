from stream_writer import StreamWriter


class KinesisWriter(StreamWriter):
    def __init__(self, broker_info):
        super(KinesisWriter, self).__init__(broker_info)

    def init_broker_stuffs(self):
        print('broker_info')

    def publish_changes_to_broker(self, formatted_changes):
        print('Kinesis publish_messages', formatted_changes)
