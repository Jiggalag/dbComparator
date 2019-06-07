import datetime

from sqlalchemy import create_engine

from helpers import df_compare_helper, dbcmp_sql_helper
from unified_comparing_class import Comparation


class Object:
    def __init__(self, sql_connection_properties, sql_comparing_properties, comparing_info):
        self.sql_connection_properties = sql_connection_properties
        self.prod_sql = sql_connection_properties.get('prod')
        self.test_sql = sql_connection_properties.get('test')
        p_host = self.prod_sql.get('host')
        p_user = self.prod_sql.get('user')
        p_password = self.prod_sql.get('password')
        p_db = self.prod_sql.get('db')
        t_host = self.prod_sql.get('host')
        t_user = self.prod_sql.get('user')
        t_password = self.prod_sql.get('password')
        t_db = self.prod_sql.get('db')
        self.prod_engine = create_engine(f'mysql+pymysql://{p_user}:{p_password}@{p_host}/{p_db}?charset=utf8mb4')
        self.test_engine = create_engine(f'mysql+pymysql://{t_user}:{t_password}@{t_host}/{t_db}?charset=utf8mb4')
        self.comparing_info = comparing_info
        self.attempts = 5
        self.comparing_step = 10000
        self.hide_columns = [
            'archived',
            'addonFields',
            'hourOfDayS',
            'dayOfWeekS',
            'impCost',
            'id'
        ]
        self.mode = 'day-sum'
        self.strings_amount = 1000
        self.client_ignored_tables = []
        self.check_schema = True
        self.depth_report_check = 7
        self.fail_with_first_error = False
        self.separate_checking = 'both'
        self.schema_columns = [
            'TABLE_CATALOG',
            'TABLE_NAME',
            'COLUMN_NAME',
            'ORDINAL_POSITION',
            'COLUMN_DEFAULT',
            'IS_NULLABLE',
            'DATA_TYPE',
            'CHARACTER_MAXIMUM_LENGTH',
            'CHARACTER_OCTET_LENGTH',
            'NUMERIC_PRECISION',
            'NUMERIC_SCALE',
            'DATETIME_PRECISION',
            'CHARACTER_SET_NAME',
            'COLLATION_NAME',
            'COLUMN_TYPE',
            'COLUMN_KEY',
            'EXTRA',
            'COLUMN_COMMENT',
            'GENERATION_EXPRESSION'
        ]
        self.only_tables = ''
        self.reports = True
        self.entities = True
        self.excluded_tables = [
            'databasechangelog',
            'download',
            'migrationhistory',
            'mntapplog',
            'reportinfo',
            'synchistory',
            'syncstage',
            'synctrace',
            'synctracelink',
            'syncpersistentjob',
            'forecaststatistics',
            'migrationhistory'
        ]
        self.table_timeout = None

        if 'retry_attempts' in sql_comparing_properties.keys():
            self.attempts = int(sql_comparing_properties.get('retry_attempts'))
        if 'comparing_step' in sql_comparing_properties.keys():
            self.comparing_step = int(sql_comparing_properties.get('comparing_step'))
        if 'skip_columns' in sql_comparing_properties.keys():
            self.hide_columns = sql_comparing_properties.get('skip_columns').split(',')
        if 'mode' in sql_comparing_properties.keys():
            self.mode = sql_comparing_properties.get('mode')
        if 'check_schema' in sql_comparing_properties.keys():
            self.check_schema = sql_comparing_properties.get('check_schema')
        if 'depth_report_check' in sql_comparing_properties.keys():
            self.depth_report_check = int(sql_comparing_properties.get('depth_report_check'))
        if 'fail_with_first_error' in sql_comparing_properties.keys():
            self.fail_with_first_error = sql_comparing_properties.get('fail_with_first_error')
        if 'schema_columns' in sql_comparing_properties.keys():
            self.schema_columns = sql_comparing_properties.get('schema_columns')
            if type(self.schema_columns) is str:
                self.schema_columns = self.schema_columns.split(',')
        if 'separateChecking' in sql_comparing_properties.keys():
            self.separate_checking = sql_comparing_properties.get('separateChecking')
        if 'only_tables' in sql_comparing_properties.keys():
            self.only_tables = sql_comparing_properties.get('only_tables').split(',')
        if 'skip_tables' in sql_comparing_properties.keys():
            self.excluded_tables = sql_comparing_properties.get('skip_tables').split(',')
        if 'path_to_logs' in sql_comparing_properties.keys():
            self.path_to_logs = sql_comparing_properties.get('path_to_logs')
        if 'send_mail_to' in sql_comparing_properties.keys():
            self.send_mail_to = sql_comparing_properties.get('send_mail_to')
        if 'logger' in sql_comparing_properties.keys():
            self.logger = sql_comparing_properties.get('logger')
        if 'table_timeout' in sql_comparing_properties.keys():
            self.table_timeout = int(sql_comparing_properties.get('table_timeout'))
            if self.table_timeout == 0:
                self.table_timeout = None
        if 'reports' in sql_comparing_properties.keys():
            self.reports = sql_comparing_properties.get('reports')
        if 'strings_amount' in sql_comparing_properties.keys():
            self.strings_amount = int(sql_comparing_properties.get('strings_amount'))
        self.sql_comparing_properties = {
            'check_schema': self.check_schema,
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'entities': self.entities,
            'fail_with_first_error': self.fail_with_first_error,
            'hide_columns': self.hide_columns,
            'mode': self.mode,
            'only_tables': self.only_tables,
            'schema_columns': self.schema_columns,
            # 'logger': self.logger,
            'reports': self.reports,
            'retry_attempts': self.attempts,
            'send_mail_to': self.send_mail_to,
            'separateChecking': self.separate_checking,
            'skip_tables': self.excluded_tables,
            'strings_amount': self.strings_amount,
            'table_timeout': self.table_timeout
        }

    def compare_data(self, start_time, service_dir, mapping, tables):
        prod_connection = dbcmp_sql_helper.DbAlchemyHelper(self.prod_sql, self.logger)

        for table in tables:
            start_table_check_time = datetime.datetime.now()
            self.logger.info(f"Table {table} processing started now...")
            is_report = dbcmp_sql_helper.is_report(table, prod_connection)

            # TODO: refactor this place! Rename onlyReports/Entities

            if 'onlyReports' in self.separate_checking and not is_report:
                continue
            if 'onlyEntities' in self.separate_checking and is_report:
                continue
            self.sql_comparing_properties.update({'service_dir': service_dir})
            compared_table = Comparation(self.prod_engine, self.test_engine, table, self.logger,
                                         self.sql_comparing_properties, self.prod_sql)
            global_break = compared_table.compare_table(is_report, mapping, start_time, self.comparing_info,
                                                        self.comparing_step)
            self.logger.info(f"Table {table} checked in {datetime.datetime.now() - start_table_check_time}...")
            if global_break:
                data_comparing_time = datetime.datetime.now() - start_time
                self.logger.warn(f'Global breaking is True. Comparing interrupted. Comparing finished in '
                                 f'{data_comparing_time}')
                return data_comparing_time
        data_comparing_time = datetime.datetime.now() - start_time
        self.logger.info(f'Comparing finished in {data_comparing_time}')
        return data_comparing_time

    def calculate_table_list(self, connection):
        if len(self.only_tables) == 1 and self.only_tables[0] == '':
            return self.comparing_info.define_table_list(self.excluded_tables, self.client_ignored_tables,
                                                         self.reports, self.entities, connection)
        else:
            return self.only_tables

    def compare_metadata(self, start_time, tables):
        prod_connection = dbcmp_sql_helper.DbAlchemyHelper(self.prod_sql, self.logger).engine
        test_connection = dbcmp_sql_helper.DbAlchemyHelper(self.test_sql, self.logger).engine
        for table in tables:
            self.logger.info(f"Check schema for table {table}...")
            schema_columns = ', '.join(self.schema_columns)
            query = (f"SELECT {schema_columns} FROM INFORMATION_SCHEMA.COLUMNS " +
                     f"WHERE TABLE_SCHEMA = 'DBNAME' AND TABLE_NAME='TABLENAME' ".replace("TABLENAME", table) +
                     f"ORDER BY COLUMN_NAME")

            prod_columns, test_columns = dbcmp_sql_helper.get_comparable_objects([prod_connection,
                                                                                  test_connection],
                                                                                 query)
            if (prod_columns is None) or (test_columns is None):
                self.logger.warn(f'Table {table} skipped because something going bad')
                continue
            diff_df = df_compare_helper.get_dataframes_diff(prod_columns, test_columns)
            if not diff_df.empty:
                self.logger.error(f"Schema of tables {table} differs!")
                # TODO: adding serializing to html file on disc
                # TODO: exclude table with problem schema from comparing
        schema_comparing_time = datetime.datetime.now() - start_time
        self.logger.info(f"Schema compared in {schema_comparing_time}")
        return schema_comparing_time
