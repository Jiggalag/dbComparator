from multiprocessing.dummy import Pool
from sqlalchemy import create_engine
import pandas as pd


class DbAlchemyConnector:
    def __init__(self, connect_parameters, logger, attempts=5, timeout=10):
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.db = connect_parameters.get('db')
        self.logger = logger
        self.attempts = attempts
        self.timeout = timeout
        self.engine = create_engine('mysql+pymysql://{}:{}@{}/'.format(self.user, self.password, self.host) +
                                    '{}?charset=utf8mb4'.format(self.db))
        self.connection = self.engine.connect()

    def get_tables(self):
        if self.connection is not None:
            show_tables = "SELECT DISTINCT(table_name) FROM information_schema.columns " \
                          "WHERE table_schema LIKE '{}';".format(self.db)
            result = list()
            for item in self.connection.execute(show_tables):
                result.append(item[0])
            return result
        else:
            return None

    # TODO: test this method
    def select(self, query):
        if self.connection is not None:
            error_count = 0
            while error_count < self.attempts:
                # TODO: probaly remove string below
                sql_query = query.replace('DBNAME', self.db)
                self.logger.debug(sql_query)
                result = list()
                for item in self.connection.execute(sql_query):
                    result.append(item[0])
                return result
        else:
            return None

    @staticmethod
    def parallel_select(connection_list, query):
        pool = Pool(2)  # TODO: remove hardcode, change to dynamically defining amount of threads
        result = pool.map((lambda x: pd.read_sql(query.replace('DBNAME', x.url.database), x)), connection_list)
        # result = pool.map((lambda x: x.execute(query.replace('DBNAME', x.url.database))), connection_list)  # TODO: add try/catch
        pool.close()
        pool.join()
        return result
