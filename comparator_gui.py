#!/usr/bin/python3
# -*- coding: utf-8 -*-
import logging
import os
from pathlib import Path
import platform
import shutil
import sys

from PyQt5.QtCore import pyqtSlot
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton
from PyQt5.QtWidgets import QRadioButton, QAction, qApp, QMainWindow

import query_constructor
import sql_comparing
import table_data
from custom_ui_elements.advanced_settings import AdvancedSettingsItem
from custom_ui_elements.clickable_lineedit import ClickableLineEdit
from custom_ui_elements.progress_window import ProgressWindow
from helpers.tables import TableManager
from helpers.config_loader import ConfigLoader
from helpers.config_saver import ConfigSaver
from sqlalchemy import MetaData, inspect
from ui_setters import UISetter
from gui_logic import Logic


class MainUI(QWidget):
    def __init__(self, status_bar):
        super().__init__()
        if "Win" in platform.system():
            self.OS = "Windows"
            # TODO: add creation of both directories below
            self.service_dir = "C:\\comparator\\"
            self.test_dir = "C:\\comparator\\test_results\\"
        else:
            self.OS = "Linux"
            self.service_dir = os.path.expanduser('~') + "/comparator/"
            Path(self.service_dir).mkdir(parents=True, exist_ok=True)
            self.test_dir = os.path.expanduser('~') + "/comparator/test_results/"
            Path(self.test_dir).mkdir(parents=True, exist_ok=True)
        self.tables = dict()
        self.tables_for_ui = dict()
        self.dbs = list()
        self.prod_db_list = list()
        self.test_db_list = list()
        self.prod_tables = dict()
        self.test_tables = dict()
        self.columns = list()
        self.prod_connect = False
        self.test_connect = False
        self.logging_level = logging.DEBUG
        self.comparing_step = 10000
        self.depth_report_check = 7
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
        self.retry_attempts = 5
        self.path_to_logs = '/tmp/tmp.log'
        self.strings_amount = 10000
        self.table_timeout = 600
        self.table_manager = None
        self.logger = logging.getLogger("dbComparator")
        self.logger.setLevel(level=self.logging_level)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self._toggle = True
        fh = logging.FileHandler(self.service_dir + 'dbcomparator.log')
        fh.setLevel(self.logging_level)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.debug('File handler added successfully')
        self.logger.info('Logger successfully initialized')
        self.statusBar = status_bar
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)

        # Labels

        self.prod_host_label = QLabel('prod.sql-host')
        self.prod_user_label = QLabel('prod.sql-user', self)
        self.prod_password_label = QLabel('prod.sql-password', self)
        self.prod_db_label = QLabel('prod.sql-db', self)
        self.prod_db_label.hide()
        self.test_host_label = QLabel('test.sql-host', self)
        self.test_user_label = QLabel('test.sql-user', self)
        self.test_password_label = QLabel('test.sql-password', self)
        self.test_db_label = QLabel('test.sql-db', self)
        self.test_db_label.hide()
        self.send_mail_to_label = QLabel('Send mail to', self)
        self.checking_mode_label = QLabel('Checking mode:', self)
        self.only_tables_label = QLabel('Only tables', self)
        self.excluded_tables_label = QLabel('Skip tables', self)
        self.hide_columns_label = QLabel('Skip columns', self)

        # Line edits

        self.logic = Logic(self)
        self.uisetter = UISetter(self)
        self.le_prod_host = QLineEdit(self)
        # self.le_prod_host.textChanged.connect(lambda: self.check_sqlhost('prod'))
        self.le_prod_host.textChanged.connect(self.logic.update_access_data)
        self.le_prod_user = QLineEdit(self)
        self.le_prod_user.textChanged.connect(self.logic.update_access_data)
        self.le_prod_password = QLineEdit(self)
        self.le_prod_password.setEchoMode(QLineEdit.Password)
        self.le_prod_password.textChanged.connect(self.logic.update_access_data)
        self.le_prod_db = ClickableLineEdit(self)
        self.le_prod_db.textChanged.connect(self.logic.update_access_data)
        self.le_prod_db.clicked.connect(self.uisetter.set_prod_db)
        self.le_prod_db.hide()
        self.le_test_host = QLineEdit(self)
        self.le_test_host.textChanged.connect(self.logic.update_access_data)
        self.le_test_user = QLineEdit(self)
        self.le_test_user.textChanged.connect(self.logic.update_access_data)
        self.le_test_password = QLineEdit(self)
        self.le_test_password.setEchoMode(QLineEdit.Password)
        self.le_test_password.textChanged.connect(self.logic.update_access_data)
        self.le_test_db = ClickableLineEdit(self)
        self.le_test_db.textChanged.connect(self.logic.update_access_data)
        self.le_test_db.clicked.connect(self.uisetter.set_test_db)
        self.le_test_db.hide()
        self.le_send_mail_to = QLineEdit(self)
        self.le_excluded_tables = ClickableLineEdit(self)
        self.le_excluded_tables.clicked.connect(UISetter.set_excluded_tables)
        self.le_only_tables = ClickableLineEdit(self)
        self.le_only_tables.clicked.connect(UISetter.set_included_tables)
        self.le_skip_columns = ClickableLineEdit(self)
        self.le_skip_columns.clicked.connect(UISetter.set_excluded_columns)

        # Checkboxes

        self.cb_enable_schema_checking = QCheckBox('Compare schema', self)
        self.cb_enable_schema_checking.toggle()
        self.cb_fail_with_first_error = QCheckBox('Only first error', self)
        self.cb_fail_with_first_error.toggle()
        self.cb_reports = QCheckBox('Reports', self)
        self.cb_reports.setChecked(self._toggle)
        self.cb_reports.clicked.connect(self.toggle)
        self.cb_entities = QCheckBox('Entities and others', self)
        self.cb_entities.setChecked(self._toggle)
        self.cb_entities.clicked.connect(self.toggle)
        self.cb_enable_dataframes = QCheckBox('Enable dataframes', self)
        self.cb_enable_dataframes.toggle()
        self.cb_enable_dataframes.setChecked(False)

        # Buttons

        self.btn_set_configuration = QPushButton('       Compare!       ', self)
        self.btn_set_configuration.setShortcut('Ctrl+G')
        self.btn_set_configuration.clicked.connect(self.start_work)
        self.btn_set_configuration.setEnabled(False)
        self.btn_clear_all = QPushButton('Clear all', self)
        self.btn_clear_all.clicked.connect(self.clear_all)
        self.btn_advanced = QPushButton('Advanced', self)
        self.btn_advanced.clicked.connect(self.advanced)
        self.btn_prod_connect = QPushButton('Check prod connection')
        self.btn_prod_connect.clicked.connect(lambda: self.check_connection('prod'))
        self.btn_prod_connect.setEnabled(False)
        self.btn_test_connect = QPushButton('Check test connection')
        self.btn_test_connect.clicked.connect(lambda: self.check_connection('test'))
        self.btn_test_connect.setEnabled(False)

        # Radiobuttons

        self.day_summary_mode = QRadioButton('Day summary')
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode = QRadioButton('Section summary')
        self.section_summary_mode.setChecked(False)
        self.detailed_mode = QRadioButton('Detailed')
        self.detailed_mode.setChecked(False)

        # Set tooltips

        self.prod_host_label.setToolTip('Input host, where prod-db located.\nExample: samaradb03.maxifier.com')
        self.le_prod_host.setToolTip(self.le_prod_host.text())
        self.prod_user_label.setToolTip('Input user for connection to prod-db.\nExample: itest')
        self.le_prod_user.setToolTip(self.le_prod_user.text())
        self.prod_password_label.setToolTip('Input password for user from prod.sql-user field')
        self.prod_db_label.setToolTip('Input prod-db name.\nExample: irving')
        self.le_prod_db.setToolTip(self.le_prod_db.text())
        self.test_host_label.setToolTip('Input host, where test-db located.\nExample: samaradb03.maxifier.com')
        self.le_test_host.setToolTip(self.le_test_host.text())
        self.test_user_label.setToolTip('Input user for connection to test-db.\nExample: itest')
        self.le_test_user.setToolTip(self.le_test_user.text())
        self.test_password_label.setToolTip('Input password for user from test.sql-user field')
        self.test_db_label.setToolTip('Input test-db name.\nExample: irving')
        self.le_test_db.setToolTip(self.le_test_db.text())
        self.cb_enable_schema_checking.setToolTip('If you set this option, program will compare also schemas of dbs')
        self.cb_fail_with_first_error.setToolTip('If you set this option, comparing will be finished after first error')
        self.send_mail_to_label.setToolTip('Add one or list of e-mails for receiving results of comparing')
        self.le_send_mail_to.setToolTip(self.le_send_mail_to.text().replace(',', ',\n'))
        self.only_tables_label.setToolTip('Set comma-separated list of tables, which should be compared')
        self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))
        self.excluded_tables_label.setToolTip('Set tables, which should not be checked')
        self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))
        self.hide_columns_label.setToolTip('Set columns, which should not be compared during checking')
        self.le_skip_columns.setToolTip(self.le_skip_columns.text().replace(',', ',\n'))
        self.checking_mode_label.setToolTip('Select type of checking')
        self.day_summary_mode.setToolTip('Compare sums of impressions for each date')
        self.section_summary_mode.setToolTip('Compare sums of impressions for each date and each section')
        self.detailed_mode.setToolTip('Compare all records from table for setted period')
        self.btn_clear_all.setToolTip('Reset all fields to default values')
        self.btn_set_configuration.setToolTip('Start comparing of dbs')

        grid.addWidget(self.prod_host_label, 0, 0)
        grid.addWidget(self.le_prod_host, 0, 1)
        grid.addWidget(self.prod_user_label, 1, 0)
        grid.addWidget(self.le_prod_user, 1, 1)
        grid.addWidget(self.prod_password_label, 2, 0)
        grid.addWidget(self.le_prod_password, 2, 1)
        grid.addWidget(self.prod_db_label, 3, 0)
        grid.addWidget(self.le_prod_db, 3, 1)
        grid.addWidget(self.btn_prod_connect, 4, 1)
        grid.addWidget(self.test_host_label, 0, 2)
        grid.addWidget(self.le_test_host, 0, 3)
        grid.addWidget(self.test_user_label, 1, 2)
        grid.addWidget(self.le_test_user, 1, 3)
        grid.addWidget(self.test_password_label, 2, 2)
        grid.addWidget(self.le_test_password, 2, 3)
        grid.addWidget(self.test_db_label, 3, 2)
        grid.addWidget(self.le_test_db, 3, 3)
        grid.addWidget(self.btn_test_connect, 4, 3)
        grid.addWidget(self.send_mail_to_label, 6, 0)
        grid.addWidget(self.le_send_mail_to, 6, 1)
        grid.addWidget(self.only_tables_label, 7, 0)
        grid.addWidget(self.le_only_tables, 7, 1)
        grid.addWidget(self.excluded_tables_label, 8, 0)
        grid.addWidget(self.le_excluded_tables, 8, 1)
        grid.addWidget(self.hide_columns_label, 9, 0)
        grid.addWidget(self.le_skip_columns, 9, 1)
        grid.addWidget(self.cb_enable_schema_checking, 10, 0)
        grid.addWidget(self.cb_fail_with_first_error, 11, 0)
        grid.addWidget(self.cb_reports, 10, 1)
        grid.addWidget(self.cb_entities, 11, 1)
        grid.addWidget(self.cb_enable_dataframes, 10, 2)
        grid.addWidget(self.checking_mode_label, 6, 3)
        grid.addWidget(self.day_summary_mode, 7, 3)
        grid.addWidget(self.section_summary_mode, 8, 3)
        grid.addWidget(self.detailed_mode, 9, 3)
        grid.addWidget(self.btn_clear_all, 5, 1)
        grid.addWidget(self.btn_advanced, 10, 3)
        grid.addWidget(self.btn_set_configuration, 11, 3)

        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        UISetter(self).set_default_values()
        self.show()

    @pyqtSlot()
    def toggle(self):
        if all([self.cb_entities.isChecked(), self.cb_reports.isChecked()]):
            self.cb_entities.setEnabled(True)
            self.cb_reports.setEnabled(True)
            self.day_summary_mode.setEnabled(True)
            self.section_summary_mode.setEnabled(True)
            self.detailed_mode.setEnabled(True)
            self.tables_for_ui = self.table_manager.common_tables.copy()
        elif self.cb_entities.isChecked():
            self.cb_entities.setEnabled(False)
            self.day_summary_mode.setEnabled(False)
            self.section_summary_mode.setEnabled(False)
            self.detailed_mode.setEnabled(False)
            self.tables_for_ui = self.table_manager.entities.copy()
        elif self.cb_reports.isChecked():
            self.cb_reports.setEnabled(False)
            self.day_summary_mode.setEnabled(True)
            self.section_summary_mode.setEnabled(True)
            self.detailed_mode.setEnabled(True)
            self.tables_for_ui = self.table_manager.reports.copy()

    def clear_all(self):
        self.le_prod_host.clear()
        self.le_prod_user.clear()
        self.le_prod_password.clear()
        self.le_prod_db.clear()
        self.le_test_host.clear()
        self.le_test_user.clear()
        self.le_test_password.clear()
        self.le_test_db.clear()
        self.le_send_mail_to.clear()
        self.le_only_tables.clear()
        UISetter(self).set_default_values()
        self.prod_db_label.hide()
        self.le_prod_db.hide()
        self.test_db_label.hide()
        self.le_test_db.hide()
        self.statusBar.showMessage('Prod disconnected, test disconnected')

    def advanced(self):
        defaults = {
            'logging_level': self.logging_level,
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'schema_columns': self.schema_columns,
            'retry_attempts': self.retry_attempts,
            'path_to_logs': self.path_to_logs,
            'table_timeout': self.table_timeout,
            'strings_amount': self.strings_amount
        }
        adv = AdvancedSettingsItem(self.OS, defaults)
        adv.exec_()
        self.logging_level = self.adv.logging_level
        self.logger.setLevel(self.logging_level)
        self.comparing_step = self.adv.comparing_step
        self.depth_report_check = self.adv.depth_report_check
        self.schema_columns = self.adv.schema_columns
        self.retry_attempts = self.adv.retry_attempts
        self.path_to_logs = self.adv.path_to_logs
        self.table_timeout = self.adv.table_timeout
        self.strings_amount = self.adv.strings_amount

        # Set tooltips

        self.le_prod_host.setToolTip(self.le_prod_host.text())
        self.le_prod_user.setToolTip(self.le_prod_user.text())
        self.le_prod_db.setToolTip(self.le_prod_db.text())
        self.le_test_host.setToolTip(self.le_test_host.text())
        self.le_test_user.setToolTip(self.le_test_user.text())
        self.le_test_db.setToolTip(self.le_test_db.text())
        self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))
        self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))
        self.le_skip_columns.setToolTip(self.le_skip_columns.text().replace(',', ',\n'))
        self.le_send_mail_to.setToolTip(self.le_send_mail_to.text().replace(',', ',\n'))

    @staticmethod
    def exit():
        sys.exit(0)

    def check_connection(self, instance_type):
        QApplication.processEvents()
        engine = self.logic.get_engine(instance_type)
        # TODO: add validation of engine
        try:
            meta = MetaData(bind=engine)
            reflection = meta.reflect()
            if reflection.tables:
                self.logic.change_bar_message(instance_type, True)
            else:
                self.logic.change_bar_message(instance_type, False)
        except AttributeError as e:
            self.logger.debug(f'It is message for debug {e}')
            inspection = inspect(engine)
            schema = inspection.get_schema_names()
            if schema:
                self.logic.change_bar_message(instance_type, True)
                if instance_type == 'prod':
                    self.prod_db_label.show()
                    self.le_prod_db.show()
                else:
                    self.test_db_label.show()
                    self.le_test_db.show()
            else:
                self.logic.change_bar_message(instance_type, False)

    @staticmethod
    def get_checkbox_state(checkbox):
        if checkbox.checkState() == 2:
            return True
        else:
            return False

    def load_configuration(self):
        cfgloader = ConfigLoader(self, self.tables, self.logger)
        cfgloader.load_configuration()
        self.check_connection('prod')
        self.check_connection('test')
        logic = Logic(self)
        prod_engine = logic.get_engine('prod')
        test_engine = logic.get_engine('test')
        if prod_engine.url.database is not None and test_engine.url.database is not None:
            self.table_manager = TableManager(prod_engine, test_engine, self.le_excluded_tables.text(), self.logger)

    def save_configuration(self):
        cfgsaver = ConfigSaver(self, self.logger)
        cfgsaver.save_configuration()

    @staticmethod
    def check_service_dir(service_dir):
        if os.path.exists(service_dir):
            shutil.rmtree(service_dir)
        os.mkdir(service_dir)

    @pyqtSlot()
    def start_work(self):
        logic = Logic(self)
        connection_dict = logic.get_sql_params()
        properties = logic.get_properties()
        if connection_dict and properties:
            if all([self.prod_connect, self.test_connect]):
                self.check_service_dir(self.service_dir)
                self.check_service_dir(self.test_dir)
                comparing_info = table_data.Info(self.logger)
                comparing_info.update_table_list("prod", self.prod_tables)
                comparing_info.update_table_list("test", self.test_tables)
                comparing_object = sql_comparing.Object(self.prod_sql_connection, self.test_sql_connection, properties,
                                                        comparing_info)
                self.logger.info('Comparing started!')
                check_schema = self.get_checkbox_state(self.cb_enable_schema_checking)
                only_tables = properties.get('only_tables')
                if only_tables:
                    result_tables = dict()
                    for table in only_tables.split(','):
                        result_tables.update({table: self.tables.get(table)})
                    self.tables = result_tables
                else:
                    for table in properties.get('skip_tables').split(','):
                        if table in self.tables:
                            self.tables.pop(table)
                            self.logger.debug(f'Deleted table {table} from self.tables list')
                enabled_dfs = self.cb_enable_dataframes.isChecked()
                progress = ProgressWindow(comparing_object, self.tables, check_schema, mapping, self.service_dir,
                                          self.logger, enabled_dfs)
                progress.exec()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.statusBar = self.statusBar()
        self.statusBar.showMessage('Prod disconnected, test disconnected')
        self.ex = MainUI(self.statusBar)
        self.setCentralWidget(self.ex)
        self.logger = self.ex.logger

        self.setGeometry(300, 300, 900, 600)
        self.setWindowTitle('dbComparator')
        self.setWindowIcon(QIcon('./resources/slowpoke.png'))
        self.show()

        # Menu

        open_action = QAction(QIcon('open.png'), '&Open', self)
        open_action.setShortcut('Ctrl+O')
        open_action.setStatusTip('Open custom file with cmp_properties')
        open_action.triggered.connect(self.ex.load_configuration)

        compare_action = QAction(QIcon('compare.png'), '&Compare', self)
        compare_action.setShortcut('Ctrl+F')
        compare_action.setStatusTip('Run comparing')
        compare_action.triggered.connect(self.ex.start_work)

        save_action = QAction(QIcon('save.png'), '&Save', self)
        save_action.setShortcut('Ctrl+S')
        save_action.setStatusTip('Save current configuration to file')
        save_action.triggered.connect(self.ex.save_configuration)

        exit_action = QAction(QIcon('exit.png'), '&Exit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.setStatusTip('Exit application')
        exit_action.triggered.connect(qApp.quit)

        menubar = self.menuBar()
        file_menu = menubar.addMenu('&File')
        file_menu.addAction(open_action)
        file_menu.addAction(save_action)
        file_menu.addAction(compare_action)
        file_menu.addAction(exit_action)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    sys.exit(app.exec_())
