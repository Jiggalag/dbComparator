import os
import os.path
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename

from helpers import converters


class Mail:
    def __init__(self, body, fromaddr, toaddr, mypass, subject, files, logger):
        self.body = body
        self.fromaddr = fromaddr
        self.toaddr = toaddr
        self.mypass = mypass
        self.subject = subject
        self.files = files
        self.logger = logger

    def generate_mail_text(self, comparing_info, sql_comparing_properties, data_comparing_time, schema_comparing_time):
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
            text = self.get_test_result_text(comparing_info)
        else:
            text = text + "It is impossible! There is no any problems founded!"
        if sql_comparing_properties.get('check_schema'):
            text = text + "Schema checked in " + str(schema_comparing_time) + "\n"
        text = text + "Dbs checked in " + str(data_comparing_time) + "\n"
        return text

    def sendmail(self):
        msg = MIMEMultipart()
        msg['From'] = self.fromaddr
        if type(self.toaddr) is list:
            msg['To'] = ', '.join(self.toaddr)
        else:
            msg['To'] = self.toaddr
        msg['Subject'] = self.subject
        msg.attach(MIMEText(self.body, 'plain'))
        if self.files is not None:
            for attachFile in self.files.split(','):
                if os.path.exists(attachFile) and os.path.isfile(attachFile):
                    with open(attachFile, 'rb') as file:
                        part = MIMEApplication(file.read(), Name=basename(attachFile))
                    part['Content-Disposition'] = f'attachment; filename="{basename(attachFile)}"'
                    msg.attach(part)
                else:
                    if attachFile.lstrip() != "":
                        self.logger.error(f"File not found {attachFile}")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        try:
            server.login(self.fromaddr, self.mypass)
            text = msg.as_string()
            server.sendmail(self.fromaddr, self.toaddr, text)
            server.quit()
        except smtplib.SMTPAuthenticationError:
            self.logger.error('Raised authentication error!')

    def get_test_result_text(self, comparing_info):
        body = self.body + "There are some problems found during checking.\n\n"
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
