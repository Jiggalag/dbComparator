from sqlalchemy import create_engine


class Logic:
    def __init__(self, main_ui):
        self.main_ui = main_ui
        self.logger = main_ui.logger

    def get_properties(self):
        check_schema = self.main_ui.get_checkbox_state(self.main_ui.cb_enable_schema_checking)
        fail_with_first_error = self.main_ui.get_checkbox_state(self.main_ui.cb_fail_with_first_error)
        reports = self.main_ui.get_checkbox_state(self.main_ui.cb_reports)
        entities = self.main_ui.get_checkbox_state(self.main_ui.cb_entities)

        if self.main_ui.day_summary_mode.isChecked():
            mode = 'day-sum'
        elif self.main_ui.section_summary_mode.isChecked():
            mode = 'section-sum'
        else:
            mode = 'detailed'

        properties_dict = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error,
            'send_mail_to': self.main_ui.le_send_mail_to.text(),
            'mode': mode,
            'skip_tables': self.main_ui.le_excluded_tables.text(),
            'hide_columns': self.main_ui.le_skip_columns.text(),
            'strings_amount': self.main_ui.strings_amount,
            # 'check_type': check_type,
            'logger': self.main_ui.logger,
            'comparing_step': self.main_ui.comparing_step,
            'depth_report_check': self.main_ui.depth_report_check,
            'schema_columns': self.main_ui.schema_columns,
            'retry_attempts': self.main_ui.retry_attempts,
            'only_tables': self.main_ui.le_only_tables.text(),
            'reports': reports,
            'entities': entities,
            'table_timeout': int(self.main_ui.table_timeout),
            'os': self.main_ui.OS
        }
        return properties_dict

    def get_sql_params(self):
        prod_host = self.main_ui.le_prod_host.text()
        prod_user = self.main_ui.le_prod_user.text()
        prod_password = self.main_ui.le_prod_password.text()
        prod_db = self.main_ui.le_prod_db.text()
        test_host = self.main_ui.le_test_host.text()
        test_user = self.main_ui.le_test_user.text()
        test_password = self.main_ui.le_test_password.text()
        test_db = self.main_ui.le_test_db.text()
        prod_dict = {
            'host': prod_host,
            'user': prod_user,
            'password': prod_password,
            'db': prod_db
        }
        test_dict = {
            'host': test_host,
            'user': test_user,
            'password': test_password,
            'db': test_db
        }
        connection_sql_parameters = {
            'prod': prod_dict,
            'test': test_dict
        }
        return connection_sql_parameters

    def update_access_data(self):
        if all([self.main_ui.le_prod_host.text(), self.main_ui.le_prod_user.text(),
                self.main_ui.le_prod_password.text()]):
            self.main_ui.btn_prod_connect.setEnabled(True)
        if all([self.main_ui.le_test_host.text(), self.main_ui.le_test_user.text(),
                self.main_ui.le_test_password.text()]):
            self.main_ui.btn_test_connect.setEnabled(True)
        if all([self.main_ui.btn_prod_connect.isEnabled(), self.main_ui.btn_test_connect.isEnabled()]):
            self.main_ui.prod_engine = self.get_engine('prod')
            self.main_ui.test_engine = self.get_engine('test')

    def get_engine(self, instance_type):
        if instance_type == 'prod':
            host = self.main_ui.le_prod_host.text()
            user = self.main_ui.le_prod_user.text()
            password = self.main_ui.le_prod_password.text()
            db = self.main_ui.le_prod_db.text()
        else:
            host = self.main_ui.le_test_host.text()
            user = self.main_ui.le_test_user.text()
            password = self.main_ui.le_test_password.text()
            db = self.main_ui.le_test_db.text()
        connection_string = f'mysql+pymysql://{user}:{password}@{host}'
        if db:
            connection_string = f'mysql+pymysql://{user}:{password}@{host}/{db}'
        return create_engine(connection_string)

    def change_bar_message(self, stage_type, value):
        current_message = self.main_ui.statusBar.currentMessage().split(', ')
        if stage_type == 'prod':
            db = f'{self.main_ui.le_prod_db.text()}'
            if db:
                if value:
                    self.main_ui.statusBar.showMessage(f'{db} connected, {current_message[1]}')
                else:
                    self.main_ui.statusBar.showMessage(f'{db} not connected, {current_message[1]}')
                self.main_ui.prod_connect = value
            else:
                host = f'{self.main_ui.le_prod_host.text()}'
                if value:
                    self.main_ui.statusBar.showMessage(f'{host} server connected, {current_message[1]}')
                else:
                    self.main_ui.statusBar.showMessage(f'{host} not connected, {current_message[1]}')
                self.main_ui.prod_connect = value
        elif stage_type == 'test':
            db = f'{self.main_ui.le_test_db.text()}'
            if db:
                if value:
                    self.main_ui.statusBar.showMessage(f'{current_message[0]}, {db} connected')
                else:
                    self.main_ui.statusBar.showMessage(f'{current_message[0]}, {db} not connected')
                self.main_ui.test_connect = value
            else:
                host = f'{self.main_ui.le_test_host.text()}'
                if value:
                    self.main_ui.statusBar.showMessage(f'{host} server connected, {current_message[1]}')
                else:
                    self.main_ui.statusBar.showMessage(f'{host} not connected, {current_message[1]}')
                self.main_ui.test_connect = value
        if all([self.main_ui.prod_connect, self.main_ui.test_connect]):
            self.main_ui.btn_set_configuration.setEnabled(True)
