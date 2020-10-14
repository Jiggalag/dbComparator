import PyQt5
from PyQt5.QtWidgets import QFileDialog, QMessageBox


class ConfigSaver:
    def __init__(self, main_ui, logger):
        self.main_ui = main_ui
        self.logger = logger

    def save_configuration(self):
        text = []
        non_verified = {}
        if self.main_ui.le_prod_host.text() != '':
            text.append(f'prod.host = {self.main_ui.le_prod_host.text()}')
        if self.main_ui.le_prod_user.text() != '':
            text.append(f'prod.user = {self.main_ui.le_prod_user.text()}')
        if self.main_ui.le_prod_password.text() != '':
            text.append(f'prod.password = {self.main_ui.le_prod_password.text()}')
        if self.main_ui.le_prod_db.text() != '':
            text.append(f'prod.dbt = {self.main_ui.le_prod_db.text()}')
        if self.main_ui.le_test_host.text() != '':
            text.append(f'test.host = {self.main_ui.le_test_host.text()}')
        if self.main_ui.le_test_user.text() != '':
            text.append(f'test.user = {self.main_ui.le_test_user.text()}')
        if self.main_ui.le_test_password.text() != '':
            text.append(f'test.password = {self.main_ui.le_test_password.text()}')
        if self.main_ui.le_test_db.text() != '':
            text.append(f'test.db = {self.main_ui.le_test_db.text()}')
        if self.main_ui.le_send_mail_to.text() != '':
            raw_array = self.main_ui.le_send_mail_to.text().split(',')
            raw_array.sort()
            recipients = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'send_mail_to = {recipients}')
        if self.main_ui.le_only_tables.text() != '':
            raw_array = self.main_ui.le_only_tables.text().split(',')
            raw_array.sort()
            table_list = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'only_tables = {table_list}')
        if self.main_ui.le_excluded_tables.text() != '':
            raw_array = self.main_ui.le_excluded_tables.text().split(',')
            raw_array.sort()
            skip_tables = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'skip_tables = {skip_tables}')
        if self.main_ui.le_skip_columns.text() != '':
            raw_array = self.main_ui.le_skip_columns.text().split(',')
            raw_array.sort()
            skip_columns = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'skip_columns = {skip_columns}')
        if self.main_ui.comparing_step != '' and self.main_ui.comparing_step != '10000':
            try:
                int(self.main_ui.comparing_step)
                text.append(f'comparing_step = {self.main_ui.comparing_step}')
            except ValueError:
                non_verified.update({'Comparing step': self.main_ui.comparing_step})
        if self.main_ui.depth_report_check != '' and self.main_ui.depth_report_check != '7':
            try:
                int(self.main_ui.depth_report_check)
                text.append(f'depth_report_check = {self.main_ui.depth_report_check}')
            except ValueError:
                non_verified.update({'Days in past': self.main_ui.depth_report_check})
        default_column_text = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                               'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                               'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,' +
                               'COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')

        if self.main_ui.schema_columns != '' and self.main_ui.schema_columns != default_column_text:
            raw_array = self.main_ui.schema_columns.split(',')
            raw_array.sort()
            schema_columns = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'schema_columns = {schema_columns}')
        if self.main_ui.retry_attempts != '' and self.main_ui.retry_attempts != '5':
            try:
                int(self.main_ui.retry_attempts)
                text.append(f'retry_attempts = {self.main_ui.retry_attempts}')
            except ValueError:
                non_verified.update({'Retry attempts': self.main_ui.retry_attempts})
        if self.main_ui.path_to_logs != '':
            text.append(f'path_to_logs = {self.main_ui.path_to_logs}')
        if self.main_ui.table_timeout != '':
            try:
                int(self.main_ui.table_timeout)
                text.append(f'table_timeout = {self.main_ui.table_timeout}')
            except ValueError:
                non_verified.update({'Timeout for single table': self.main_ui.table_timeout})
        if self.main_ui.strings_amount != '':
            try:
                int(self.main_ui.strings_amount)
                text.append(f'string_amount = {self.main_ui.strings_amount}')
            except ValueError:
                non_verified.update({'Amount of stored unique strings': self.main_ui.strings_amount})
        if non_verified:
            text = ''
            for item in non_verified.keys():
                text = f'{text}\n{item}: {non_verified.get(item)}'
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                (f"Incorrect value(s):\n{text}\n\n" +
                                 "Please, input a number!"),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False
        if self.main_ui.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = True')
        if not self.main_ui.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = False')
        if self.main_ui.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = True')
        if not self.main_ui.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = False')
        if self.main_ui.day_summary_mode.isChecked():
            text.append('mode = day-sum')
        elif self.main_ui.section_summary_mode.isChecked():
            text.append('mode = section-sum')
        elif self.main_ui.detailed_mode.isChecked():
            text.append('mode = detailed')
        text.append(f'logging_level = {self.main_ui.logging_level}')
        file_name, _ = QFileDialog.getSaveFileName(PyQt5.QtWidgets.QFileDialog(), "QFileDialog.getSaveFileName()", "",
                                                   "All Files (*);;Text Files (*.txt)")
        if file_name:
            with open(file_name, 'w') as file:
                file.write('\n'.join(text))
            self.logger.info(f'Configuration successfully saved to {file_name}')  # TODO: fix this
