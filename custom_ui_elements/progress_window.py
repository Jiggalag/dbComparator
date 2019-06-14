import datetime

from PyQt5.QtWidgets import QDialog, QProgressBar, QGridLayout, QLabel


class ProgressWindow(QDialog):
    def __init__(self, comparing_object, tables, check_schema):
        super(ProgressWindow, self).__init__()
        self.setGeometry(50, 50, 500, 300)
        grid = QGridLayout()
        grid.setSpacing(10)
        self.setLayout(grid)
        self.comparing_object = comparing_object
        self.tables = tables
        self.check_schema = check_schema
        self.progress_schema = QProgressBar(self)
        self.progress_data = QProgressBar(self)
        self.start_time = datetime.datetime.now()
        schema_checking = QLabel('Schema checking')
        data_checking = QLabel('Data checking')
        grid.addWidget(schema_checking, 0, 0)
        grid.addWidget(self.progress_schema, 0, 1)
        grid.addWidget(data_checking, 1, 0)
        grid.addWidget(self.progress_data, 1, 1)
        if not self.check_schema:
            schema_checking.setVisible(False)
            self.progress_schema.setVisible(False)

        self.show()
        self.start()

    def start(self):
        self.completed = 0
        part = 100 / len(self.tables)
        if self.check_schema:
            self.setWindowTitle("Comparing metadata...")
            for table in self.tables:
                self.completed = part * (self.tables.index(table) + 1)
                self.progress_schema.setValue(self.completed)
                self.comparing_object.compare_table_metadata(self.start_time, table)
        else:
            self.logger.info("Schema checking disabled...")
        self.setWindowTitle("Comparing data...")
        schema_checking_time = datetime.datetime.now() - self.start_time
        for table in self.tables:
            self.completed = part * (self.tables.index(table) + 1)
            self.progress_data.setValue(self.completed)
            self.comparing_object.compare_data(service_dir='service_dir', mapping='mapping', table=table)
        data_comparing_time = datetime.datetime.now() - schema_checking_time