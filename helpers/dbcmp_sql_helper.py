from multiprocessing.dummy import Pool
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


class DbAlchemyHelper:
    def __init__(self, connect_parameters, logger, **kwargs):
        self.meta = sqlalchemy.schema.MetaData()
        self.read_timeout = None
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.db = connect_parameters.get('db')
        self.db_not_found = False
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}')
        self.connection = self.engine.connect()
        self.insp = sqlalchemy.inspect(self.engine)
        self.db_list = self.insp.get_schema_names()
        if self.db is not None and self.db_list:
            if self.db not in self.db_list:
                self.db_not_found = True
            else:
                self.connection.execute(f'USE {self.db};')
                self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db}')
        self.logger = logger
        self.hide_columns = [
            'archived',
            'addonFields',
            'hourOfDayS',
            'dayOfWeekS',
            'impCost',
            'id']
        self.attempts = 5
        self.mode = 'detailed'
        self.comparing_step = 10000
        self.excluded_tables = []
        self.client_ignored_tables = []
        self.check_schema = True
        self.check_depth = 7
        self.quick_fall = False
        self.separate_checking = "both"
        self.schema_columns = [
            "TABLE_CATALOG",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "COLUMN_TYPE",
            "COLUMN_KEY",
            "EXTRA",
            "COLUMN_COMMENT",
            "GENERATION_EXPRESSION"
        ]
        self.set_keyvalues(**kwargs)

    def select(self, query):
        if self.connection is not None:
            error_count = 0
            while error_count < self.attempts:
                sql_query = query.replace('DBNAME', self.db)
                self.logger.debug(sql_query)
                result = list()
                for item in self.engine.execute(sql_query):
                    result.append(item)
                return result
        else:
            return None

    def set_keyvalues(self, **kwargs):
        for key in list(kwargs.keys()):
            if 'hideColumns' in key:
                self.hide_columns = kwargs.get(key)
            if 'attempts' in key:
                self.attempts = int(kwargs.get(key))
            if 'mode' in key:
                self.mode = kwargs.get(key)
            if 'comparingStep' in key:
                self.comparing_step = kwargs.get(key)
            if 'excludedTables' in key:
                self.excluded_tables = kwargs.get(key)  # TODO: add split?
            if 'clientIgnoredTables' in key:
                self.client_ignored_tables = kwargs.get(key)  # TODO: add split?
            if 'enableSchemaChecking' in key:
                self.check_schema = kwargs.get(key)
            if 'depthReportCheck' in key:
                self.check_depth = kwargs.get(key)
            if 'failWithFirstError' in key:
                self.quick_fall = kwargs.get(key)
            if 'schemaColumns' in key:
                self.schema_columns = kwargs.get(key)  # TODO: add split?
            if 'separateChecking' in key:
                self.separate_checking = kwargs.get(key)
            if 'read_timeout' in key:
                self.read_timeout = int(kwargs.get(key))
            return self

    def get_tables(self):
        if self.connection is not None and not self.db_not_found:
            show_tables = (f"SELECT DISTINCT(table_name) FROM information_schema.columns "
                           f"WHERE table_schema LIKE '{self.db}';")
            result = list()
            for item in self.connection.execute(show_tables):
                result.append(item[0])
            return result
        else:
            return None

    def get_tables_columns(self):
        if self.connection is not None and not self.db_not_found:
            query = (f"SELECT table_name, column_name FROM information_schema.columns "
                     f"WHERE table_schema LIKE '{self.db}';")
            result = dict()
            for item in self.connection.execute(query):
                table = item[0]
                column = item[1]
                if table not in result:
                    result.update({table: [column]})
                else:
                    new_value = result.get(table)
                    new_value.append(column)
                    result.update({table: new_value})
            final_result = dict()
            for table in result:
                columns = result.get(table)
                if all(['dt' in columns, 'impressions' in columns, 'clicks' in columns]):
                    final_result.update({table: {'columns': columns, 'is_report': True}})
                else:
                    final_result.update({table: {'columns': columns, 'is_report': False}})
            return final_result

    def get_column_list(self, table):
        if self.connection is not None:
            query = (f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}' "
                     f"AND table_schema = '{self.db}';")
            self.logger.debug(query)
            columns = list()
            for i in self.connection.execute(query):
                element = i[0].lower()  # TODO: get rid of this hack
                columns.append(element)
            for column in self.hide_columns:
                # TODO: get rid of this hack
                if column.replace('_', '') in columns:
                    columns.remove(column)
            if not columns:
                return ""
            column_string = ','.join(columns)
            return column_string.lower().split(',')
        else:
            return None

    @staticmethod
    def parallel_select(connection_list, query):
        pool = Pool(2)  # TODO: remove hardcode, change to dynamically defining amount of threads
        result = pool.map((lambda x: pd.read_sql(query.replace('DBNAME', x.url.database), x)), connection_list)
        pool.close()
        pool.join()
        return result


def get_amount_records(table, dates, sql_connection_list):
    if dates is None:
        query = f"SELECT COUNT(*) FROM `{table}`;"
    else:
        query = f"SELECT COUNT(*) FROM `{table}` WHERE dt >= '{dates[0]}';"
    result = get_comparable_objects(sql_connection_list, query)
    return result[0].values[0][0], result[1].values[0][0]


# TODO: strongly refactor this code!
def get_raw_objects(connection_list, query):
    result = DbAlchemyHelper.parallel_select(connection_list, query)
    if (result[0] is None) or (result[1] is None):
        return None, None
    else:
        return result[0], result[1]


def get_raw_object(connection, query):
    return pd.read_sql(query.replace('DBNAME', connection.url.database), connection)


# returns list for easy convertation to set
# TODO: remove this interlayer
def get_comparable_objects(connection_list, query):
    result = get_raw_objects(connection_list, query)
    if len(result[0].index) != len(result[1].index):
        return result[0], result[1]
    result[0].sort_index(axis=1)
    result[1].sort_index(axis=1)
    return result[0], result[1]


def get_column_list_for_sum(set_column_list):
    column_list_with_sums = []
    for item in set_column_list.split(","):
        if "clicks" in item or "impressions" in item:
            column_list_with_sums.append("sum(" + item + ")")
        else:
            column_list_with_sums.append(item)
    return column_list_with_sums
