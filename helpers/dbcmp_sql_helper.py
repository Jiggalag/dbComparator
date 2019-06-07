import sqlalchemy
from sqlalchemy import create_engine

from helpers import dbHelperAlchemy


class DbAlchemyHelper:
    def __init__(self, connect_parameters, logger, **kwargs):
        self.meta = sqlalchemy.schema.MetaData()
        self.read_timeout = None
        self.host = connect_parameters.get('host')
        self.user = connect_parameters.get('user')
        self.password = connect_parameters.get('password')
        self.db = connect_parameters.get('db')
        if self.db is None:
            self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}')
        else:
            self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/'
                                        f'{self.db}?charset=utf8mb4')
        try:
            self.insp = sqlalchemy.inspect(self.engine)
        except sqlalchemy.exc.InternalError as e:
            self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}')
            self.insp = sqlalchemy.inspect(self.engine)
            logger.error(e.args[0])
        self.db_list = self.insp.get_schema_names()
        self.connection = self.engine.connect()
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

    def get_tables(self):
        if self.connection is not None:
            show_tables = (f"SELECT DISTINCT(table_name) FROM information_schema.columns "
                           f"WHERE table_schema LIKE '{self.db}';")
            result = list()
            for item in self.connection.execute(show_tables):
                result.append(item[0])
            return result
        else:
            return None

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


def get_amount_records(table, dates, sql_connection_list):
    if dates is None:
        query = f"SELECT COUNT(*) FROM `{table}`;"
    else:
        query = f"SELECT COUNT(*) FROM `{table}` WHERE dt >= '{dates[0]}';"
    result = get_comparable_objects(sql_connection_list, query)
    return result[0].values[0][0], result[1].values[0][0]


# TODO: strongly refactor this code!
def get_raw_objects(connection_list, query):
    result = dbHelperAlchemy.DbAlchemyConnector.parallel_select(connection_list, query)
    if (result[0] is None) or (result[1] is None):
        return None, None
    else:
        return result[0], result[1]


def prepare_column_mapping(sql_connection, logger):
    column_dict = {}
    query_get_column = (f"SELECT column_name, referenced_table_name FROM "
                        f"INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE constraint_name NOT LIKE "
                        f"'PRIMARY' AND referenced_table_name "
                        f"IS NOT NULL AND table_schema = '{sql_connection.db}';")
    logger.debug(query_get_column)
    raw_column_list = sql_connection.select(query_get_column)
    for item in raw_column_list:
        column_dict.update({"f`{item.lower()}`": f"`{item.lower()}`"})
    return column_dict


# returns list for easy convertation to set
# TODO: remove this interlayer
def get_comparable_objects(connection_list, query):
    result = get_raw_objects(connection_list, query)
    return result[0], result[1]


def get_dates(connection_list, query):
    result = list(get_raw_objects(connection_list, query))
    dates = []
    for bulk in result:
        server_dates = []
        for item in bulk:
            server_dates.append(next(iter(item.values())))
        dates.append(server_dates)
    return dates[0], dates[1]


def collapse_item(target_list):
    if len(target_list) == 1:
        return list(target_list[0].values())
    else:
        return target_list


def get_column_list_for_sum(set_column_list):
    column_list_with_sums = []
    for item in set_column_list.split(","):
        if "clicks" in item or "impressions" in item:
            column_list_with_sums.append("sum(" + item + ")")
        else:
            column_list_with_sums.append(item)
    return column_list_with_sums


def is_report(table, connection):
    query = f"DESCRIBE {table};"
    result = connection.select(query)
    columns = list()
    for column in result:
        columns.append(column._row[0])
    if all(['dt' in columns, 'impressions' in columns, 'clicks' in columns]):
        return True
    else:
        return False