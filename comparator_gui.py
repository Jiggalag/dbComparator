#!/usr/bin/python3
# -*- coding: utf-8 -*-
import logging
import os
from pathlib import Path
import platform
import shutil
import sys

import PyQt5
import pymysql
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, QWidget, QLineEdit, QCheckBox, QPushButton, QMessageBox
from PyQt5.QtWidgets import QFileDialog, QRadioButton, QAction, qApp, QMainWindow

import query_constructor
import sql_comparing
import table_data
from custom_ui_elements.advanced_settings import AdvancedSettingsItem
from custom_ui_elements.clickable_items_view import ClickableItemsView
from custom_ui_elements.clickable_lineedit import ClickableLineEdit
from custom_ui_elements.progress_window import ProgressWindow
from custom_ui_elements.radiobutton_items_view import RadiobuttonItemsView
from helpers import dbcmp_sql_helper

if "Win" in platform.system():
    operating_system = "Windows"
else:
    operating_system = "Linux"
if "Linux" in operating_system:
    propertyFile = os.getcwd() + "/resources/properties/sqlComparator.properties"
else:
    propertyFile = os.getcwd() + "\\resources\\properties\\sqlComparator.properties"


class MainUI(QWidget):
    def __init__(self, status_bar):
        super().__init__()
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
        self.logger = logging.getLogger("dbComparator")
        self.logger.setLevel(level=self.logging_level)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self._toggle = True
        self.OS = operating_system
        if self.OS == "Windows":
            # TODO: add creation of both directories below
            self.service_dir = "C:\\comparator\\"
            self.test_dir = "C:\\comparator\\test_results\\"
        else:
            self.service_dir = os.path.expanduser('~') + "/comparator/"
            Path(self.service_dir).mkdir(parents=True, exist_ok=True)
            self.test_dir = os.path.expanduser('~') + "/comparator/test_results/"
            Path(self.test_dir).mkdir(parents=True, exist_ok=True)
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

        self.le_prod_host = QLineEdit(self)
        self.le_prod_host.textChanged.connect(lambda: self.check_sqlhost('prod'))
        self.le_prod_user = QLineEdit(self)
        self.le_prod_user.textChanged.connect(lambda: self.check_sqlhost('prod'))
        self.le_prod_password = QLineEdit(self)
        self.le_prod_password.setEchoMode(QLineEdit.Password)
        self.le_prod_password.textChanged.connect(lambda: self.check_sqlhost('prod'))
        self.le_prod_db = ClickableLineEdit(self)
        self.le_prod_db.textChanged.connect(lambda: self.check_db('prod'))
        self.le_prod_db.clicked.connect(self.set_prod_db)
        self.le_prod_db.hide()
        self.le_test_host = QLineEdit(self)
        self.le_test_host.textChanged.connect(lambda: self.check_sqlhost('test'))
        self.le_test_user = QLineEdit(self)
        self.le_test_user.textChanged.connect(lambda: self.check_sqlhost('test'))
        self.le_test_password = QLineEdit(self)
        self.le_test_password.setEchoMode(QLineEdit.Password)
        self.le_test_password.textChanged.connect(lambda: self.check_sqlhost('test'))
        self.le_test_db = ClickableLineEdit(self)
        self.le_test_db.textChanged.connect(lambda: self.check_db('test'))
        self.le_test_db.clicked.connect(self.set_test_db)
        self.le_test_db.hide()
        self.le_send_mail_to = QLineEdit(self)
        self.le_excluded_tables = ClickableLineEdit(self)
        self.le_excluded_tables.clicked.connect(self.set_excluded_tables)
        self.le_only_tables = ClickableLineEdit(self)
        self.le_only_tables.clicked.connect(self.set_included_tables)
        self.le_skip_columns = ClickableLineEdit(self)
        self.le_skip_columns.clicked.connect(self.set_excluded_columns)

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

        # Radiobuttons

        self.day_summary_mode = QRadioButton('Day summary')
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode = QRadioButton('Section summary')
        self.section_summary_mode.setChecked(False)
        self.detailed_mode = QRadioButton('Detailed')
        self.detailed_mode.setChecked(False)

        self.set_default_values()

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
        grid.addWidget(self.test_host_label, 0, 2)
        grid.addWidget(self.le_test_host, 0, 3)
        grid.addWidget(self.test_user_label, 1, 2)
        grid.addWidget(self.le_test_user, 1, 3)
        grid.addWidget(self.test_password_label, 2, 2)
        grid.addWidget(self.le_test_password, 2, 3)
        grid.addWidget(self.test_db_label, 3, 2)
        grid.addWidget(self.le_test_db, 3, 3)
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
        self.show()

    def get_only_reports(self):
        result = dict()
        for table in self.tables:
            if self.tables.get(table).get('is_report'):
                columns = self.tables.get(table).get('columns')
                result.update({table: columns})
            else:
                continue
        return result

    def get_only_entities(self):
        result = dict()
        for table in self.tables:
            if not self.tables.get(table).get('is_report'):
                columns = self.tables.get(table).get('columns')
                result.update({table: columns})
            else:
                continue
        return result

    @pyqtSlot()
    def toggle(self):
        if all([self.cb_entities.isChecked(), self.cb_reports.isChecked()]):
            self.cb_entities.setEnabled(True)
            self.cb_reports.setEnabled(True)
            self.day_summary_mode.setEnabled(True)
            self.section_summary_mode.setEnabled(True)
            self.detailed_mode.setEnabled(True)
            self.tables_for_ui = self.tables.copy()
        elif self.cb_entities.isChecked():
            self.cb_entities.setEnabled(False)
            self.day_summary_mode.setEnabled(False)
            self.section_summary_mode.setEnabled(False)
            self.detailed_mode.setEnabled(False)
            self.tables_for_ui = self.get_only_entities()
        elif self.cb_reports.isChecked():
            self.cb_reports.setEnabled(False)
            self.day_summary_mode.setEnabled(True)
            self.section_summary_mode.setEnabled(True)
            self.detailed_mode.setEnabled(True)
            self.tables_for_ui = self.get_only_reports()

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
        self.set_default_values()
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
        self.adv = AdvancedSettingsItem(operating_system, defaults)
        self.adv.exec_()
        self.logging_level = self.adv.logging_level
        self.logger.setLevel(self.logging_level)
        self.comparing_step = self.adv.comparing_step
        self.depth_report_check = self.adv.depth_report_check
        self.schema_columns = self.adv.schema_columns
        self.retry_attempts = self.adv.retry_attempts
        self.path_to_logs = self.adv.path_to_logs
        self.table_timeout = self.adv.table_timeout
        self.strings_amount = self.adv.strings_amount

    def calculate_table_list(self):
        if all([self.prod_connect, self.test_connect]):
            tables = list(set(self.prod_tables.keys()) & set(self.test_tables.keys()))
            tables.sort()
            for table in tables:
                prod_columns = self.prod_tables.get(table).get('columns')
                test_columns = self.test_tables.get(table).get('columns')
                if prod_columns == test_columns:
                    self.tables.update({table: self.prod_tables.get(table)})
                else:
                    self.logger.error(f"There is different columns for table {table}.")
                    self.logger.warning(f"Table {table} excluded from comparing")
                    prod_uniq_columns = set(prod_columns) - set(test_columns)
                    test_uniq_columns = set(test_columns) - set(prod_columns)
                    if prod_uniq_columns:
                        self.logger.info(f"Uniq columns for prod {table}: {prod_uniq_columns}")
                    if test_uniq_columns:
                        self.logger.info(f"Uniq columns for test {table}: {test_uniq_columns}")
            self.tables_for_ui = self.tables.copy()
            self.calculate_excluded_columns()

    def calculate_excluded_columns(self):
        excluded_tables = self.le_excluded_tables.text().split(',')
        self.columns = list()
        for table in self.tables_for_ui:
            if table not in excluded_tables:
                columns = self.tables_for_ui.get(table)
                for column in columns:
                    if column not in self.columns:
                        self.columns.append(column)
        self.columns.sort()

    def set_default_values(self):
        self.le_excluded_tables.setText('databasechangelog,download,migrationhistory,mntapplog,reportinfo,' +
                                        'synchistory,syncstage,synctrace,synctracelink,syncpersistentjob,' +
                                        'forecaststatistics,migrationhistory')
        self.le_excluded_tables.setCursorPosition(0)
        self.le_skip_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')
        self.le_skip_columns.setCursorPosition(0)
        self.comparing_step = 10000
        self.depth_report_check = 7
        self.schema_columns = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                               'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                               'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                               'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')
        self.retry_attempts = 5
        if operating_system == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.path_to_logs = 'C:\\DbComparator\\DbComparator.log'
        elif operating_system == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.path_to_logs = log_path + 'DbComparator.log'
        self.table_timeout = 5
        self.strings_amount = 1000
        self.cb_enable_schema_checking.setChecked(True)
        self.cb_fail_with_first_error.setChecked(True)
        self.day_summary_mode.setChecked(True)
        self.section_summary_mode.setChecked(False)
        self.detailed_mode.setChecked(False)
        self.logging_level = 'DEBUG'

    def set_prod_db(self):
        prod_db = self.le_prod_db.text()
        select_db_view = RadiobuttonItemsView(self.prod_db_list, prod_db)
        select_db_view.exec_()
        self.le_prod_db.setText(select_db_view.selected_db)
        self.le_prod_db.setToolTip(self.le_prod_db.text())

    def set_test_db(self):
        test_db = self.le_test_db.text()
        select_db_view = RadiobuttonItemsView(self.test_db_list, test_db)
        select_db_view.exec_()
        self.le_test_db.setText(select_db_view.selected_db)
        self.le_test_db.setToolTip(self.le_test_db.text())

    def set_excluded_tables(self):
        if all([self.prod_connect, self.test_connect]):
            tables_to_skip = self.le_excluded_tables.text().split(',')
            skip_tables_view = ClickableItemsView(self.tables_for_ui, tables_to_skip)
            skip_tables_view.exec_()
            self.le_excluded_tables.setText(','.join(skip_tables_view.selected_items))
            self.le_excluded_tables.setToolTip(self.le_excluded_tables.text().replace(',', ',\n'))
            self.calculate_excluded_columns()

    def set_included_tables(self):
        if all([self.prod_connect, self.test_connect]):
            tables_to_include = self.le_only_tables.text().split(',')
            only_tables_view = ClickableItemsView(self.tables_for_ui, tables_to_include)
            only_tables_view.exec_()
            self.le_only_tables.setText(','.join(only_tables_view.selected_items))
            self.le_only_tables.setToolTip(self.le_only_tables.text().replace(',', ',\n'))

    def set_excluded_columns(self):
        columns_to_skip = self.le_skip_columns.text().split(',')
        skip_columns = ClickableItemsView(self.columns, columns_to_skip)
        skip_columns.exec_()
        self.le_skip_columns.setText(','.join(skip_columns.selected_items))
        self.le_skip_columns.setToolTip(self.le_skip_columns.text().replace(',', ',\n'))

    @staticmethod
    def set_value(widget, value):
        widget.setText(value)
        widget.setCursorPosition(0)

    def load_configuration(self):
        current_dir = f'{os.getcwd()}/resources/properties/'
        fname = QFileDialog.getOpenFileName(PyQt5.QtWidgets.QFileDialog(), 'Open file', current_dir)[0]
        self.clear_all()
        try:
            with open(fname, 'r') as file:
                data = file.read()
                for record in data.split('\n'):
                    string = record.replace(' ', '')
                    value = string[string.find('=') + 1:]
                    if 'prod.host' in string:
                        self.set_value(self.le_prod_host, value)
                    if 'prod.user' in string:
                        self.set_value(self.le_prod_user, value)
                    if 'prod.password' in string:
                        self.set_value(self.le_prod_password, value)
                    if 'prod.db' in string:
                        self.le_prod_db.show()
                        self.prod_db_label.show()
                        self.set_value(self.le_prod_db, value)
                    elif 'test.host' in string:
                        self.set_value(self.le_test_host, value)
                    elif 'test.user' in string:
                        self.set_value(self.le_test_user, value)
                    elif 'test.password' in string:
                        self.set_value(self.le_test_password, value)
                    elif 'test.db' in string:
                        self.test_db_label.show()
                        self.le_test_db.show()
                        self.set_value(self.le_test_db, value)
                    elif 'only_tables' in string:
                        tmp = ''
                        for table in value.split(','):
                            if self.tables:
                                if table in self.tables:
                                    tmp = tmp + table + ','
                                else:
                                    self.logger.warning(f'Table {table} excluded from only_table section '
                                                     f'because it differs on both databases')
                        self.set_value(self.le_only_tables, tmp)
                    elif 'skip_tables' in string:
                        self.set_value(self.le_excluded_tables, value)
                    elif 'comparing_step' in string:
                        self.comparing_step = value
                    elif 'depth_report_check' in string:
                        self.depth_report_check = value
                    elif 'schema_columns' in string:
                        self.schema_columns = value
                    elif 'retry_attempts' in string:
                        self.retry_attempts = value
                    elif 'path_to_logs' in string:
                        self.path_to_logs = value
                    elif 'send_mail_to' in string:
                        self.set_value(self.le_send_mail_to, value)
                    elif 'skip_columns' in string:
                        self.set_value(self.le_skip_columns, value)
                    elif 'compare_schema' in string:
                        compare_schema = value
                        if compare_schema == 'True':
                            if self.cb_enable_schema_checking.isChecked():
                                pass
                            else:
                                self.cb_enable_schema_checking.setChecked(True)
                        else:
                            if self.cb_enable_schema_checking.isChecked():
                                self.cb_enable_schema_checking.setChecked(False)
                            else:
                                pass
                    elif 'fail_with_first_error' in string:
                        only_first_error = value
                        if only_first_error == 'True':
                            if self.cb_fail_with_first_error.isChecked():
                                pass
                            else:
                                self.cb_fail_with_first_error.setChecked(True)
                        else:
                            if self.cb_fail_with_first_error.isChecked():
                                self.cb_fail_with_first_error.setChecked(False)
                            else:
                                pass
                    elif 'reports' in string:
                        reports = value
                        if reports == 'True':
                            if self.cb_reports.isChecked():
                                pass
                            else:
                                self.cb_reports.setChecked(True)
                        else:
                            if self.cb_reports.isChecked():
                                self.cb_reports.setChecked(False)
                            else:
                                pass
                    elif 'entities' in string:
                        entities = value
                        if entities == 'True':
                            if self.cb_entities.isChecked():
                                pass
                            else:
                                self.cb_entities.setChecked(True)
                        else:
                            if self.cb_entities.isChecked():
                                self.cb_entities.setChecked(False)
                            else:
                                pass
                    elif 'logging_level' in string:
                        self.logging_level = value
                    elif 'table_timeout' in string:
                        self.table_timeout = value
                    elif 'mode' in string:
                        mode = value
                        if mode == 'day-sum':
                            self.day_summary_mode.setChecked(True)
                        elif mode == 'section-sum':
                            self.section_summary_mode.setChecked(True)
                        else:
                            self.detailed_mode.setChecked(True)
        except FileNotFoundError as err:
            self.logger.warning(f'File not found, or, probably, you just pressed cancel. Warn: {err.args[1]}')

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

    def save_configuration(self):
        text = []
        non_verified = {}
        if self.le_prod_host.text() != '':
            text.append(f'prod.host = {self.le_prod_host.text()}')
        if self.le_prod_user.text() != '':
            text.append(f'prod.user = {self.le_prod_user.text()}')
        if self.le_prod_password.text() != '':
            text.append(f'prod.password = {self.le_prod_password.text()}')
        if self.le_prod_db.text() != '':
            text.append(f'prod.dbt = {self.le_prod_db.text()}')
        if self.le_test_host.text() != '':
            text.append(f'test.host = {self.le_test_host.text()}')
        if self.le_test_user.text() != '':
            text.append(f'test.user = {self.le_test_user.text()}')
        if self.le_test_password.text() != '':
            text.append(f'test.password = {self.le_test_password.text()}')
        if self.le_test_db.text() != '':
            text.append(f'test.db = {self.le_test_db.text()}')
        if self.le_send_mail_to.text() != '':
            raw_array = self.le_send_mail_to.text().split(',')
            raw_array.sort()
            recepients = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'send_mail_to = {recepients}')
        if self.le_only_tables.text() != '':
            raw_array = self.le_only_tables.text().split(',')
            raw_array.sort()
            table_list = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'only_tables = {table_list}')
        if self.le_excluded_tables.text() != '':
            raw_array = self.le_excluded_tables.text().split(',')
            raw_array.sort()
            skip_tables = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'skip_tables = {skip_tables}')
        if self.le_skip_columns.text() != '':
            raw_array = self.le_skip_columns.text().split(',')
            raw_array.sort()
            skip_columns = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'skip_columns = {skip_columns}')
        if self.comparing_step != '' and self.comparing_step != '10000':
            try:
                int(self.comparing_step)
                text.append(f'comparing_step = {self.comparing_step}')
            except ValueError:
                non_verified.update({'Comparing step': self.comparing_step})
        if self.depth_report_check != '' and self.depth_report_check != '7':
            try:
                int(self.depth_report_check)
                text.append(f'depth_report_check = {self.depth_report_check}')
            except ValueError:
                non_verified.update({'Days in past': self.depth_report_check})
        default_column_text = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                               'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                               'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,' +
                               'COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,GENERATION_EXPRESSION')

        if self.schema_columns != '' and self.schema_columns != default_column_text:
            raw_array = self.schema_columns.split(',')
            raw_array.sort()
            schema_columns = str(raw_array).strip('[]').replace("'", "").replace(' ', '')
            text.append(f'schema_columns = {schema_columns}')
        if self.retry_attempts != '' and self.retry_attempts != '5':
            try:
                int(self.retry_attempts)
                text.append(f'retry_attempts = {self.retry_attempts}')
            except ValueError:
                non_verified.update({'Retry attempts': self.retry_attempts})
        if self.path_to_logs != '':
            text.append(f'path_to_logs = {self.path_to_logs}')
        if self.table_timeout != '':
            try:
                int(self.table_timeout)
                text.append(f'table_timeout = {self.table_timeout}')
            except ValueError:
                non_verified.update({'Timeout for single table': self.table_timeout})
        if self.strings_amount != '':
            try:
                int(self.strings_amount)
                text.append(f'string_amount = {self.strings_amount}')
            except ValueError:
                non_verified.update({'Amount of stored uniq strings': self.strings_amount})
        if non_verified:
            text = ''
            for item in non_verified.keys():
                text = f'{text}\n{item}: {non_verified.get(item)}'
            QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Error',
                                (f"Incorrect value(s):\n{text}\n\n" +
                                 "Please, input a number!"),
                                QMessageBox.Ok, QMessageBox.Ok)
            return False
        if self.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = True')
        if not self.cb_enable_schema_checking.isChecked():
            text.append('compare_schema = False')
        if self.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = True')
        if not self.cb_fail_with_first_error.isChecked():
            text.append('fail_with_first_error = False')
        if self.day_summary_mode.isChecked():
            text.append('mode = day-sum')
        elif self.section_summary_mode.isChecked():
            text.append('mode = section-sum')
        elif self.detailed_mode.isChecked():
            text.append('mode = detailed')
        text.append(f'logging_level = {self.logging_level}')
        file_name, _ = QFileDialog.getSaveFileName(PyQt5.QtWidgets.QFileDialog(), "QFileDialog.getSaveFileName()",  "",
                                                   "All Files (*);;Text Files (*.txt)")
        if file_name:
            with open(file_name, 'w') as file:
                file.write('\n'.join(text))
            print(f'Configuration successfully saved to {file_name}')  # TODO: fix this

    @staticmethod
    def exit():
        sys.exit(0)

    def change_bar_message(self, stage_type, value):
        current_message = self.statusBar.currentMessage().split(', ')
        if stage_type == 'prod':
            db = f'{self.le_prod_db.text()}'
            if value:
                self.statusBar.showMessage(f'{db} connected, {current_message[1]}')
                self.prod_connect = True
            else:
                self.statusBar.showMessage(f'{db} disconnected, {current_message[1]}')
                self.prod_connect = False
        elif stage_type == 'test':
            db = f'{self.le_test_db.text()}'
            if value:
                self.statusBar.showMessage(f'{current_message[0]}, {db} connected')
                self.test_connect = True
            else:
                self.statusBar.showMessage(f'{current_message[0]}, {db} disconnected')
                self.test_connect = False
        if all([self.prod_connect, self.test_connect]):
            self.btn_set_configuration.setEnabled(True)
            self.calculate_table_list()

    def check_db(self, instance_type):
        if instance_type not in ['prod', 'test']:
            self.logger.critical(f'Unknown instance type {instance_type}, fix this in code!')
            sys.exit(1)  # TODO: use fail-fast
        if instance_type == 'prod':
            host = self.le_prod_host.text()
            user = self.le_prod_user.text()
            password = self.le_prod_password.text()
            db = self.le_prod_db.text()
        else:
            host = self.le_test_host.text()
            user = self.le_test_user.text()
            password = self.le_test_password.text()
            db = self.le_test_db.text()
        if all([host, user, password, db]):
            access_data = {
                'host': host,
                'user': user,
                'password': password,
                'db': db
            }
            if instance_type == 'prod':
                self.prod_sql_connection = dbcmp_sql_helper.DbAlchemyHelper(access_data, self.logger)
                self.prod_tables = self.prod_sql_connection.get_tables_columns()
                if self.prod_tables is not None:
                    self.logger.info(
                        f"Connection to {access_data.get('host')}/{access_data.get('db')} established successfully!")
                    self.change_bar_message('prod', True)
                    return True
                else:
                    self.logger.warning(f"Connection to {access_data.get('host')}/{access_data.get('db')} failed")
                    QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                        f"Connection to {access_data.get('host')}/{access_data.get('db')} "
                                        f"failed\n\n", QMessageBox.Ok, QMessageBox.Ok)
                    self.change_bar_message('prod', False)
                    return False
            else:
                self.test_sql_connection = dbcmp_sql_helper.DbAlchemyHelper(access_data, self.logger)
                self.test_tables = self.test_sql_connection.get_tables_columns()
                if self.test_tables is not None:
                    self.logger.info(f"Connection to db {access_data.get('host')}/"
                                     f"{access_data.get('db')} established successfully!")
                    self.change_bar_message('test', True)
                    return True
                else:
                    self.logger.warning(f"Connection to {access_data.get('host')}/{access_data.get('db')} failed")
                    QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                        f"Connection to {access_data.get('host')}/{access_data.get('db')} failed\n\n",
                                        QMessageBox.Ok, QMessageBox.Ok)
                    self.change_bar_message('test', False)
                    return False
        else:
            if not host:
                self.logger.debug(f'Field {instance_type}.sql-host is not filled')
            elif not user:
                self.logger.debug(f'Field {instance_type}.sql-user is not filled')
            elif not password:
                self.logger.debug(f'Field {instance_type}.sql-password is not filled')
            else:
                self.logger.debug(f'Field {instance_type}.sql-db is not filled')


    def check_sqlhost(self, instance_type):
        self.check_db(instance_type)
        if instance_type not in ['prod', 'test']:
            self.logger.critical(f'Unknown instance type {instance_type}, fix this in code!')
            sys.exit(1)  # TODO: use fail-fast
        if instance_type == 'prod':
            host = self.le_prod_host.text()
            user = self.le_prod_user.text()
            password = self.le_prod_password.text()
        else:
            host = self.le_test_host.text()
            user = self.le_test_user.text()
            password = self.le_test_password.text()
        if all([host, user, password]):
            access_data = {
                'host': host,
                'user': user,
                'password': password
            }
            try:
                if instance_type == 'prod':
                    self.prod_db_list = dbcmp_sql_helper.DbAlchemyHelper(access_data, self.logger).db_list
                    self.prod_db_label.show()
                    self.le_prod_db.show()
                    self.logger.info(f"Connection to {access_data.get('host')} established successfully!")
                    self.change_bar_message('prod', True)
                    return True
                else:
                    self.test_db_list = dbcmp_sql_helper.DbAlchemyHelper(access_data, self.logger).db_list
                    self.test_db_label.show()
                    self.le_test_db.show()
                    self.logger.info(f"Connection to {access_data.get('host')} established successfully!")
                    self.change_bar_message('test', True)
                    return True
            except pymysql.OperationalError as err:
                self.logger.warning(f"Connection to {access_data.get('host')} failed\n\n{err.args[1]}")
                QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                    f"Connection to {access_data.get('host')} failed\n\n{err.args[1]}",
                                    QMessageBox.Ok, QMessageBox.Ok)
                return False
            except pymysql.InternalError as err:
                self.logger.warning(f"Connection to {access_data.get('host')} failed\n\n{err.args[1]}")
                QMessageBox.warning(PyQt5.QtWidgets.QMessageBox(), 'Warning',
                                    f"Connection to {access_data.get('host')} failed\n\n{err.args[1]}",
                                    QMessageBox.Ok, QMessageBox.Ok)
                return False
        else:
            return False

    def get_sql_params(self):
        empty_fields = []
        if not self.le_prod_host.text():
            empty_fields.append('prod.host')
        if not self.le_prod_user.text():
            empty_fields.append('prod.user')
        if not self.le_prod_password.text():
            empty_fields.append('prod.password')
        if not self.le_prod_db.text():
            empty_fields.append('prod.db')
        if not self.le_test_host.text():
            empty_fields.append('test.host')
        if not self.le_test_user.text():
            empty_fields.append('test.user')
        if not self.le_test_password.text():
            empty_fields.append('test.password')
        if not self.le_test_db.text():
            empty_fields.append('test.db')
        if empty_fields:
            if len(empty_fields) == 1:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error', "Please, set this parameter:\n\n" +
                                     "\n".join(empty_fields), QMessageBox.Ok, QMessageBox.Ok)
                return False
            else:
                QMessageBox.question(PyQt5.QtWidgets.QMessageBox(), 'Error', "Please, set this parameters:\n\n" +
                                     "\n".join(empty_fields), QMessageBox.Ok, QMessageBox.Ok)
                return False
        else:
            prod_host = self.le_prod_host.text()
            prod_user = self.le_prod_user.text()
            prod_password = self.le_prod_password.text()
            prod_db = self.le_prod_db.text()
            test_host = self.le_test_host.text()
            test_user = self.le_test_user.text()
            test_password = self.le_test_password.text()
            test_db = self.le_test_db.text()
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

    @staticmethod
    def get_checkbox_state(checkbox):
        if checkbox.checkState() == 2:
            return True
        else:
            return False

    def get_properties(self):
        check_schema = self.get_checkbox_state(self.cb_enable_schema_checking)
        fail_with_first_error = self.get_checkbox_state(self.cb_fail_with_first_error)
        reports = self.get_checkbox_state(self.cb_reports)
        entities = self.get_checkbox_state(self.cb_entities)

        if self.day_summary_mode.isChecked():
            mode = 'day-sum'
        elif self.section_summary_mode.isChecked():
            mode = 'section-sum'
        else:
            mode = 'detailed'

        path_to_logs = self.path_to_logs
        if path_to_logs == '':
            path_to_logs = None

        properties_dict = {
            'check_schema': check_schema,
            'fail_with_first_error': fail_with_first_error,
            'send_mail_to': self.le_send_mail_to.text(),
            'mode': mode,
            'skip_tables': self.le_excluded_tables.text(),
            'hide_columns': self.le_skip_columns.text(),
            'strings_amount': self.strings_amount,
            # 'check_type': check_type,
            'logger': self.logger,
            'comparing_step': self.comparing_step,
            'depth_report_check': self.depth_report_check,
            'schema_columns': self.schema_columns,
            'retry_attempts': self.retry_attempts,
            'only_tables': self.le_only_tables.text(),
            'reports': reports,
            'entities': entities,
            'table_timeout': int(self.table_timeout),
            'os': operating_system
        }
        return properties_dict

    @staticmethod
    def check_service_dir(service_dir):
        if os.path.exists(service_dir):
            shutil.rmtree(service_dir)
        os.mkdir(service_dir)

    @pyqtSlot()
    def start_work(self):
        connection_dict = self.get_sql_params()
        properties = self.get_properties()
        if connection_dict and properties:
            if all([self.prod_connect, self.test_connect]):
                self.check_service_dir(self.service_dir)
                self.check_service_dir(self.test_dir)
                comparing_info = table_data.Info(self.logger)
                comparing_info.update_table_list("prod", self.prod_sql_connection.get_tables())
                comparing_info.update_table_list("test", self.test_sql_connection.get_tables())
                mapping = query_constructor.prepare_column_mapping(self.prod_sql_connection, self.logger)
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
