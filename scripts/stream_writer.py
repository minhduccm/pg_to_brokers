from abc import ABCMeta
from abc import abstractmethod


class StreamWriter(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(StreamWriter, self).__init__()

    @abstractmethod
    def init_broker_stuffs(self, logger):
        pass

    @abstractmethod
    def publish_changes_to_broker(self, formatted_changes, logger):
        pass
