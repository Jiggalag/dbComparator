import sys
from PyQt5.QtCore import QThread
from sqlalchemy import exc, MetaData, inspect


class ConnChecker(QThread):
    def __init__(self, engine, logger):
        super(QThread, self).__init__()
        self.logger = logger
        self.engine = engine
        self.connected = False
        self.engine = None
        self.metadata = None
        self.dbs = None
        self.tables = None

    def run(self):
        attempt = 0
        while not self.connected:
            if attempt < 5:
                try:
                    attempt += 1
                    self.metadata = MetaData(bind=self.engine, reflect=True)
                    inspection = inspect(self.engine)
                    self.dbs = inspection.get_schema_names()
                    if self.db is None:
                        message = f'Successfully connected to {self.host}'
                        self.connected = True
                    else:
                        message = f'Successfully connected to {self.host}/{self.db}'
                        self.tables = self.metadata.tables
                        self.connected = True
                    self.logger.info(f'{message} with given credentials')
                except exc.InternalError as err:
                    self.logger.error(err)
                    self.quit()
                except:
                    self.logger.error(sys.exc_info())
