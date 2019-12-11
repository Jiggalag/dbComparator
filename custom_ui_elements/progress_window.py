import datetime

from PyQt5.QtWidgets import QDialog, QProgressBar, QGridLayout, QLabel, QApplication


class ProgressWindow(QDialog):
    def __init__(self, comparing_object, tables, check_schema, mapping, service_dir, logger):
        super(ProgressWindow, self).__init__()
        self.setGeometry(50, 50, 500, 300)
        grid = QGridLayout()
        grid.setSpacing(5)
        self.setLayout(grid)
        self.comparing_object = comparing_object
        self.tables = tables
        self.check_schema = check_schema
        self.mapping = mapping
        self.service_dir = service_dir
        self.progress_schema = QProgressBar(self)
        self.progress_data = QProgressBar(self)
        self.schema_label = QLabel()
        self.schema_label.setFixedWidth(300)
        self.data_label = QLabel()
        self.data_label.setFixedWidth(300)
        self.start_time = datetime.datetime.now()
        self.logger = logger
        schema_checking = QLabel('Schema checking')
        data_checking = QLabel('Data checking')
        grid.addWidget(schema_checking, 0, 0)
        grid.addWidget(self.progress_schema, 0, 1)
        grid.addWidget(self.schema_label, 1, 0)
        grid.addWidget(data_checking, 2, 0)
        grid.addWidget(self.progress_data, 2, 1)
        grid.addWidget(self.data_label, 3, 0)
        if not self.check_schema:
            schema_checking.setVisible(False)
            self.progress_schema.setVisible(False)
            self.schema_label.setVisible(False)
        self.show()
        self.start()

    def start(self):
        self.completed = 0
        part = 100 / len(self.tables)
        if self.check_schema:
            self.setWindowTitle("Comparing metadata...")
            for table in self.tables:
                self.completed = part * (list(self.tables.keys()).index(table) + 1)
                self.progress_schema.setValue(self.completed)
                self.schema_label.setText(f'Processing of {table} table...')
                self.comparing_object.compare_table_metadata(table)
                QApplication.processEvents()
                # TODO: add record to log with total time of schema checking
            self.schema_label.setText(f'Schemas successfully compared...')
        else:
            self.logger.info("Schema checking disabled...")
        self.setWindowTitle("Comparing data...")
        schema_checking_time = datetime.datetime.now() - self.start_time
        for table in self.tables:
            self.completed = part * (list(self.tables.keys()).index(table) + 1)
            self.progress_data.setValue(self.completed)
            self.data_label.setText(f'Processing of {table} table...')
            is_report = self.tables.get(table).get('is_report')
            self.comparing_object.compare_data(service_dir=self.service_dir, mapping=self.mapping, table=table,
                                               is_report=is_report)
            QApplication.processEvents()
        self.data_label.setText(f'Data successfully compared...')
        data_comparing_time = datetime.datetime.now() - schema_checking_time
        # TODO: add record to log with total time of data checking
