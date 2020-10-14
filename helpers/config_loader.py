import os

import PyQt5
from PyQt5.QtWidgets import QFileDialog


class ConfigLoader:
    def __init__(self, main_ui, tables, logger):
        self.main_ui = main_ui
        self.tables = tables
        self.logger = logger

    def load_configuration(self):
        current_dir = f'{os.getcwd()}/resources/properties/'
        file_name = QFileDialog.getOpenFileName(PyQt5.QtWidgets.QFileDialog(), 'Open file', current_dir)[0]
        self.main_ui.clear_all()
        try:
            with open(file_name, 'r') as file:
                data = file.read()
                variables = {
                    'comparing_step': self.main_ui.comparing_step,
                    'depth_report_check': self.main_ui.depth_report_check,
                    'schema_columns': self.main_ui.schema_columns,
                    'retry_attempts': self.main_ui.retry_attempts,
                    'path_to_logs': self.main_ui.path_to_logs,
                    'logging_level': self.main_ui.logging_level,
                    'table_timeout': self.main_ui.table_timeout
                }
                for record in data.split('\n'):
                    string = record.replace(' ', '')
                    value = string[string.find('=') + 1:]
                    if ('prod' in string) or ('test' in string):
                        self.load_sql_credentials(string, value)
                    elif 'skip_tables' in string:
                        self.set_value(self.main_ui.le_excluded_tables, value)
                    elif 'only_tables' in string:
                        self.load_only_tables_field(value)
                    elif string in ['send_mail_to', 'skip_tables', 'skip_columns']:
                        self.load_to_widgets(string, value)
                    elif string in variables:
                        self.load_variables(variables, string, value)
                    elif string in ['compare_schema', 'fail_with_first_error', 'reports', 'entities']:
                        self.load_checkboxes(string, value)
                    elif 'mode' in string:
                        self.load_mode(value)
        except FileNotFoundError as err:
            self.logger.warning(f'File not found, or, probably, you just pressed cancel. Warn: {err.args[1]}')

    @staticmethod
    def set_value(widget, value):
        widget.setText(value)
        widget.setCursorPosition(0)

    def load_sql_credentials(self, string, value):
        mapping = {
            'prod.host': self.main_ui.le_prod_host,
            'prod.user': self.main_ui.le_prod_user,
            'prod.password': self.main_ui.le_prod_password,
            'prod.db': [self.main_ui.le_prod_db, self.main_ui.prod_db_label],
            'test.host': self.main_ui.le_test_host,
            'test.user': self.main_ui.le_test_user,
            'test.password': self.main_ui.le_test_password,
            'test.db': [self.main_ui.le_test_db, self.main_ui.test_db_label]
        }
        for ui_key in mapping:
            if ('prod.db' in string) or ('test.db' in string):
                if string in ['prod.db', 'test.db']:
                    for item in mapping.get(string):
                        item.show()
                    self.set_value(mapping.get(string)[0], value)
            elif ui_key in string:
                self.set_value(mapping.get(ui_key), value)

    def load_only_tables_field(self, value):
        tmp = ''
        for table in value.split(','):
            if self.tables:
                if table in self.tables:
                    tmp = tmp + table + ','
                else:
                    self.logger.warning(f'Table {table} excluded from only_table section '
                                        f'because it differs on both databases')
        self.set_value(self.main_ui.le_only_tables, tmp)

    # TODO: clarify and fix method!
    @staticmethod
    def load_variables(variables, string, value):
        print('stop')
        # variables.get(string) = value

    def load_checkboxes(self, string, value):
        mapping = {
            'compare_schema': self.main_ui.cb_enable_schema_checking,
            'fail_with_first_error': self.main_ui.cb_fail_with_first_error,
            'reports': self.main_ui.cb_reports,
            'entities': self.main_ui.cb_entities
        }
        if value == 'True':
            if not mapping.get(string).isChecked():
                mapping.get(string).setChecked(True)
        else:
            if mapping.get(string).isChecked():
                mapping.get(string).setChecked(False)

    def load_to_widgets(self, string, value):
        mapping = {
            'send_mail_to': self.main_ui.le_send_mail_to,
            'skip_tables': self.main_ui.le_excluded_tables,
            'skip_columns': self.main_ui.le_skip_columns
        }
        self.set_value(mapping.get(string), value)

    def load_mode(self, value):
        mapping = {
            'day-sum': self.main_ui.day_summary_mode,
            'section-sum': self.main_ui.section_summary_mode,
            'detailed': self.main_ui.detailed_mode
        }
        mapping.get(value).setChecked(True)
