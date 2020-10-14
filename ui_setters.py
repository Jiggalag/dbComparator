import os

from custom_ui_elements.clickable_items_view import ClickableItemsView
from custom_ui_elements.radiobutton_items_view import RadiobuttonItemsView
from sqlalchemy import inspect


class UISetter:
    def __init__(self, main_ui):
        self.main_ui = main_ui
        self.logic = self.main_ui.logic
        self.logger = main_ui.logger

    def set_prod_db(self):
        prod_db = self.main_ui.le_prod_db.text()
        if not prod_db:
            prod_db = None
        if not self.main_ui.prod_db_list:
            self.main_ui.prod_db_list = inspect(self.main_ui.prod_engine).get_schema_names()
        select_db_view = RadiobuttonItemsView(self.main_ui.prod_db_list, prod_db)
        select_db_view.exec_()
        self.main_ui.le_prod_db.setText(select_db_view.selected_db)
        self.main_ui.le_prod_db.setToolTip(self.main_ui.le_prod_db.text())
        self.main_ui.logic.change_bar_message('prod', True)

    def set_test_db(self):
        test_db = self.main_ui.le_test_db.text()
        if not test_db:
            test_db = None
        if not self.main_ui.test_db_list:
            self.main_ui.test_db_list = inspect(self.main_ui.test_engine).get_schema_names()
        select_db_view = RadiobuttonItemsView(self.main_ui.test_db_list, test_db)
        select_db_view.exec_()
        self.main_ui.le_test_db.setText(select_db_view.selected_db)
        self.main_ui.le_test_db.setToolTip(self.main_ui.le_test_db.text())
        self.main_ui.logic.change_bar_message('test', True)

    def set_excluded_tables(self):
        if all([self.main_ui.prod_connect, self.main_ui.test_connect]):
            tables_to_skip = self.main_ui.le_excluded_tables.text().split(',')
            skip_tables_view = ClickableItemsView(self.main_ui.tables_for_ui, tables_to_skip)
            skip_tables_view.exec_()
            self.main_ui.le_excluded_tables.setText(','.join(skip_tables_view.selected_items))
            self.main_ui.le_excluded_tables.setToolTip(self.main_ui.le_excluded_tables.text().replace(',', ',\n'))

    def set_included_tables(self):
        if all([self.main_ui.prod_connect, self.main_ui.test_connect]):
            tables_to_include = self.main_ui.le_only_tables.text().split(',')
            only_tables_view = ClickableItemsView(self.main_ui.tables_for_ui, tables_to_include)
            only_tables_view.exec_()
            self.main_ui.le_only_tables.setText(','.join(only_tables_view.selected_items))
            self.main_ui.le_only_tables.setToolTip(self.main_ui.le_only_tables.text().replace(',', ',\n'))

    def set_excluded_columns(self):
        columns_to_skip = self.main_ui.le_skip_columns.text().split(',')
        skip_columns = ClickableItemsView(self.main_ui.columns, columns_to_skip)
        skip_columns.exec_()
        self.main_ui.le_skip_columns.setText(','.join(skip_columns.selected_items))
        self.main_ui.le_skip_columns.setToolTip(self.main_ui.le_skip_columns.text().replace(',', ',\n'))

    def set_default_values(self):
        self.main_ui.le_excluded_tables.setText('databasechangelog,download,migrationhistory,mntapplog,reportinfo,' +
                                                'synchistory,syncstage,synctrace,synctracelink,syncpersistentjob,' +
                                                'forecaststatistics,migrationhistory')
        self.main_ui.le_excluded_tables.setCursorPosition(0)
        self.main_ui.le_skip_columns.setText('archived,addonFields,hourOfDayS,dayOfWeekS,impCost,id')
        self.main_ui.le_skip_columns.setCursorPosition(0)
        self.main_ui.comparing_step = 10000
        self.main_ui.depth_report_check = 7
        self.main_ui.schema_columns = ('TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,' +
                                       'IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,' +
                                       'NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,' +
                                       'COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,COLUMN_COMMENT,' +
                                       'GENERATION_EXPRESSION')
        self.main_ui.retry_attempts = 5
        if self.main_ui.OS == 'Windows':
            # TODO: add defining disc
            if not os.path.exists('C:\\DbComparator\\'):
                os.mkdir('C:\\DbComparator\\')
            self.main_ui.path_to_logs = 'C:\\DbComparator\\DbComparator.log'
        elif self.main_ui.OS == 'Linux':
            log_path = os.path.expanduser('~') + '/DbComparator/'
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            self.main_ui.path_to_logs = log_path + 'DbComparator.log'
        self.main_ui.table_timeout = 5
        self.main_ui.strings_amount = 1000
        self.main_ui.cb_enable_schema_checking.setChecked(True)
        self.main_ui.cb_fail_with_first_error.setChecked(True)
        self.main_ui.day_summary_mode.setChecked(True)
        self.main_ui.section_summary_mode.setChecked(False)
        self.main_ui.detailed_mode.setChecked(False)
        self.main_ui.logging_level = 'DEBUG'
