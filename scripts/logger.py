import logging


class Logger(object):
    def __init__(
        self,
        log_info
    ):
        super(Logger, self).__init__()
        self.log_info = log_info

    def get_logger(self):
        log_info = self.log_info
        log_path = '.'
        log_file_name = 'postgresql_to_brokers.log'
        if log_info is not None and \
            'log_path' in log_info and \
                'log_file_name' in log_info:
            log_path = log_info['log_path']
            log_file_name = log_info['log_file_name']

        log_format = '%(asctime)s - %(name)s: [%(levelname)s]: %(message)s'
        logger = logging.getLogger('POSTGRESQL_TO_BROKERS')
        logger.basicConfig = logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            filename='{log_path}/{log_file_name}'
            .format(
                log_path=log_path,
                log_file_name=log_file_name
            ),
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        return logger
