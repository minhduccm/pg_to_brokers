from abc import ABCMeta
from abc import abstractmethod


class StreamWriter(object):

    __metaclass__ = ABCMeta

    def __init__(self, broker_info):
        super(StreamWriter, self).__init__()
        self.broker_info = broker_info

    @abstractmethod
    def init_broker_stuffs(self):
        pass

    @abstractmethod
    def publish_changes_to_broker(self, formatted_changes):
        pass
