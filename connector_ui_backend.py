import datetime
import os
import os.path
import shutil
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename

import query_constructor
import sql_comparing
import table_data
from helpers import converters


class Backend:
    def __init__(self, prod_connect, test_connect, sql_connection_properties, sql_comparing_properties):
        self.prod_sql_connection = prod_connect
        self.test_sql_connection = test_connect
        self.sql_connection_properties = sql_connection_properties
        self.sql_comparing_properties = sql_comparing_properties  # TODO: refactor - move dict to object
        self.logger = sql_comparing_properties.get('logger')
        self.OS = sql_comparing_properties.get('os')
        if self.OS == "Windows":
            self.service_dir = "C:\\comparator"
            self.test_dir = "C:\\dbComparator\\"
        else:
            self.service_dir = "/tmp/comparator/"
            self.test_dir = os.getcwd() + "/test_results/"
        check_service_dir(self.service_dir)
        check_service_dir(self.test_dir)
        self.comparing_info = table_data.Info(self.logger)
        self.comparing_info.update_table_list("prod", self.prod_sql_connection.get_tables())
        self.comparing_info.update_table_list("test", self.test_sql_connection.get_tables())

        self.start_time = datetime.datetime.now()
        self.logger.info("Start processing!")
        self.mapping = query_constructor.prepare_column_mapping(self.prod_sql_connection, self.logger)
        self.comparing_object = sql_comparing.Object(self.prod_sql_connection, self.test_sql_connection,
                                                     self.sql_connection_properties, self.sql_comparing_properties,
                                                     self.comparing_info)
        self.tables = self.comparing_object.calculate_table_list(self.prod_sql_connection)

    def run_comparing(self):
        if self.sql_comparing_properties.get('check_schema'):
            schema_comparing_time = self.comparing_object.compare_metadata(self.start_time, self.tables)
        else:
            self.logger.info("Schema checking disabled...")
            schema_comparing_time = None
        data_comparing_time = self.comparing_object.compare_data(self.start_time, self.service_dir, self.mapping,
                                                                 self.tables)
        subject = "[Test] Check databases"
        text = generate_mail_text(self.comparing_info, self.sql_comparing_properties,
                                  data_comparing_time, schema_comparing_time)


def check_service_dir(service_dir):
    if os.path.exists(service_dir):
        shutil.rmtree(service_dir)
    os.mkdir(service_dir)


def generate_mail_text(comparing_info, sql_comparing_properties, data_comparing_time, schema_comparing_time):
    text = "Initial conditions:\n\n"
    if sql_comparing_properties.get('check_schema'):
        text = text + "1. Schema checking enabled.\n"
    else:
        text = text + "1. Schema checking disabled.\n"
    if sql_comparing_properties.get('fail_with_first_error'):
        text = text + "2. Failed with first founded error.\n"
    else:
        text = text + "2. Find all errors\n"
        text = text + "3. Report checkType is " + sql_comparing_properties.get('mode') + "\n\n"
    if any([comparing_info.empty, comparing_info.diff_data, comparing_info.no_crossed_tables,
            comparing_info.prod_uniq_tables, comparing_info.test_uniq_tables]):
        text = get_test_result_text(text, comparing_info)
    else:
        text = text + "It is impossible! There is no any problems founded!"
    if sql_comparing_properties.get('check_schema'):
        text = text + "Schema checked in " + str(schema_comparing_time) + "\n"
    text = text + "Dbs checked in " + str(data_comparing_time) + "\n"
    return text


def sendmail(body, fromaddr, toaddr, mypass, subject, files, logger):
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    if type(toaddr) is list:
        msg['To'] = ', '.join(toaddr)
    else:
        msg['To'] = toaddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    if files is not None:
        for attachFile in files.split(','):
            if os.path.exists(attachFile) and os.path.isfile(attachFile):
                with open(attachFile, 'rb') as file:
                    part = MIMEApplication(file.read(), Name=basename(attachFile))
                part['Content-Disposition'] = f'attachment; filename="{basename(attachFile)}"'
                msg.attach(part)
            else:
                if attachFile.lstrip() != "":
                    logger.error(f"File not found {attachFile}")
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    try:
        server.login(fromaddr, mypass)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
    except smtplib.SMTPAuthenticationError:
        logger.error('Raised authentication error!')


def get_test_result_text(body, comparing_info):
    body = body + "There are some problems found during checking.\n\n"
    if comparing_info.empty:
        body = body + "Tables, empty in both dbs:\n" + ",".join(comparing_info.empty) + "\n\n"
    if comparing_info.prod_empty:
        body = body + "Tables, empty on production db:\n" + ",".join(comparing_info.prod_empty) + "\n\n"
    if comparing_info.test_empty:
        body = body + "Tables, empty on test db:\n" + ",".join(comparing_info.test_empty) + "\n\n"
    if comparing_info.diff_data:
        body = body + "Tables, which have any difference:\n" + ",".join(comparing_info.diff_data) + "\n\n"
    if list(set(comparing_info.empty).difference(set(comparing_info.no_crossed_tables))):
        body = body + "Report tables, which have no crossing dates:\n" + ",".join(
            list(set(comparing_info.empty).difference(set(comparing_info.no_crossed_tables)))) + "\n\n"
    if comparing_info.get_uniq_tables("prod"):
        body = body + "Tables, which unique for production db:\n" + ",".join(
            converters.convert_to_list(comparing_info.prod_uniq_tables)) + "\n\n"
    if comparing_info.get_uniq_tables("test"):
        body = body + "Tables, which unique for test db:\n" + ",".join(
            converters.convert_to_list(comparing_info.test_uniq_tables)) + "\n\n"
    return body
