from sqlalchemy import MetaData, exc


class TableManager:
    def __init__(self, prod_engine, test_engine, excluded_tables, logger):
        self.logger = logger
        self.prod_engine = prod_engine
        self.test_engine = test_engine
        self.prod_meta = None
        self.test_meta = None
        try:
            self.prod_meta = MetaData(bind=self.prod_engine, reflect=True)
        except exc.OperationalError as e:
            self.logger.error(e)
        try:
            self.test_meta = MetaData(bind=self.test_engine, reflect=True)
        except exc.OperationalError as e:
            self.logger.error(e)
        if self.prod_meta is not None and self.test_meta is not None:
            self.common_tables = self.get_common_tables()
            self.entities, self.reports = self.__classify_tables()
            self.excluded_tables = excluded_tables
        else:
            self.common_tables = None
            self.entities = None
            self.reports = None
            self.excluded_tables = None

    def __classify_tables(self):
        entities = dict()
        reports = dict()
        report_columns = {'dt', 'impressions', 'clicks'}
        for table in self.common_tables:
            print('stop')
            if not (report_columns - set(self.common_tables.get(table).columns.keys())) and 'report' in table:
                reports.update({table: self.common_tables.get(table)})
            else:
                entities.update({table: self.common_tables.get(table)})
        return entities, reports

    def get_common_tables(self):
        common_tables = set(self.prod_meta.tables).intersection(self.test_meta.tables)
        prod_unique_tables = set(self.prod_meta.tables) - set(self.test_meta.tables)
        test_unique_tables = set(self.test_meta.tables) - set(self.prod_meta.tables)
        if prod_unique_tables:
            self.logger.warning(
                f"Tables, unique for db {self.prod_engine.url.database}: {','.join(prod_unique_tables)}")
        if test_unique_tables:
            self.logger.warning(
                f"Tables, unique for db {self.test_engine.url.database}: {','.join(test_unique_tables)}")
        result = dict()
        for table in common_tables:
            result.update({table: self.prod_meta.tables.get(table)})
        return result
