import datetime
import sys


class Logger:
    def __init__(self, logging_type, log_file=None):
        self.types = {
            'CRITICAL': 50,
            'ERROR': 40,
            'WARNING': 30,
            'INFO': 20,
            'DEBUG': 10
        }

        if logging_type not in self.types:
            logging_type.decode('utf8')
            print('Unregistered type of message: {}'.format(logging_type))
            sys.stdout.flush()
            self.type = None
        else:
            self.type = logging_type
        self.log_file = log_file

    def _msg(self, message, msgtype):
        if self.types.get(self.type) <= self.types.get(msgtype):
            print('{} [{}] {}'.format(str(datetime.datetime.now()), msgtype, message))
            sys.stdout.flush()
            if self.log_file is not None:
                with open(self.log_file, 'a') as file:
                    file.write('{} [{}] {}\n'.format(str(datetime.datetime.now()), msgtype, message))

    def critical(self, message):
        self._msg(message, 'CRITICAL')

    def error(self, message):
        self._msg(message, 'ERROR')

    def warn(self, message):
        self._msg(message, 'WARN')

    def info(self, message):
        self._msg(message, 'INFO')

    def debug(self, message):
        self._msg(message, 'DEBUG')
