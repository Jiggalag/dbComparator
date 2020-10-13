import sys
from PyQt5.QtCore import QThread
from sqlalchemy import create_engine, exc, MetaData, inspect


class ConnChecker(QThread):
    def __init__(self, host, user, password, logger, db=None):
        super(QThread, self).__init__()
        self.host = host
        self.user = user
        self.password = password
        self.logger = logger
        self.db = db
        if self.db is None:
            self.connection_string = f'mysql+pymysql://{self.user}:{self.password}@{self.host}'
        else:
            self.connection_string = f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db}'
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
                    self.engine = create_engine(self.connection_string).connect()
                    if self.db is None:
                        self.metadata = MetaData(bind=self.engine, reflect=False)
                    else:
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
                    self.logger.error('There is some error!')
                    self.logger.error(sys.exc_info())
