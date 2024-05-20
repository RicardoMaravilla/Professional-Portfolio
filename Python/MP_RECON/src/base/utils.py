"""
Name: utils.py
Description: Methods to use during process execution
Created by: Álvaro González <alvaro.glez@ibm.com>
Created Date: 2020/12/22
Modification:
    date        owner       description
"""
# *- encoding = utf-8 -*
import os
import base.constants as constants
import logging
import smtplib
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from socket import gethostname

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

logger = logging.getLogger()


def send_mail(recipient, subject, body_message):
    """
    This method is used to send notification mails

    Parameters
        recipient (str): mail address' recipient(s),
        subject (str): subject message,
        body_message (str): body message

    Returns
        (bool) On success, True. On failure, False
    """
    sender = f"ods@{str(gethostname())}"
    mail = MIMEMultipart()

    mail["From"] = sender
    mail["To"] = recipient
    mail["Date"] = formatdate(localtime=True)
    mail["Subject"] = subject
    body_message = MIMEText(body_message)
    mail.attach(body_message)

    try:
        server = smtplib.SMTP(constants.SMTP_SERVER)
        server.ehlo()
        server.starttls()
        server.sendmail(sender, recipient, str(mail))
        server.close()
    except Exception as e:
        logger.error("Error sending mail.")
        return False

    logger.info("Message sent correctly")
    return True


def send_mail_attachment(recipient, subject, body_message, attachment):
    """
    This method is used to send notification mails

    Parameters
        recipient (str): mail address' recipient(s),
        subject (str): subject message,
        body_message (str): body message
        attachment (srt array): files to attach
    Returns
        (bool) On success, True. On failure, False
    """
    sender = f"ods@{str(gethostname())}"
    mail = MIMEMultipart()

    mail["From"] = sender
    mail["To"] = recipient
    mail["Date"] = formatdate(localtime=True)
    mail["Subject"] = subject
    body_message = MIMEText(body_message)
    mail.attach(body_message)

    for file in attachment:
        pre_file = MIMEText(open(file).read())
        pre_file.add_header(
            "Content-Disposition",
            "attachment; filename= {0}".format(os.path.basename(file)),
        )
        mail.attach(pre_file)

    try:
        server = smtplib.SMTP(constants.SMTP_SERVER)
        server.ehlo()
        server.starttls()
        server.sendmail(sender, recipient, str(mail))
        server.close()
    except Exception as e:
        logger.error("Error sending mail.")
        return False

    logger.info("Message sent correctly")
    return True


def get_workdir(environment, process_type, process_name="", project_name=""):
    """
    This method is used to get the working directory

    Parameters
        environment (str): environment,
        process_type (str): which type of process,
        process (str) [Optional]: name of the process

    Returns
        workdir (str) on success, False in failure
    """
    if process_type == "SAP":
        if environment == "FVT":
            workdir = "/ods/sap_fvt/extract/"
        elif environment == "UAT":
            workdir = "/ods/sap_uat/extract/"
        elif environment == "DEV" or environment == "PROD":
            workdir = "/ods/sap/extract/"
        else:
            return False
    elif process_type == "RDH":
        if environment == "FVT":
            workdir = f"/cdb_oper/fvt/{process_name}"
        elif environment == "UAT":
            workdir = f"/cdb_oper/uat/{process_name}"
        elif environment == "DEV" or environment == "PROD":
            workdir = f"/cdb_oper/prd/{process_name}"
        else:
            return False
    elif process_type == "OCE":
        if environment == "FVT":
            workdir = f"/{project_name}/oce_fvt/"
        elif environment == "UAT":
            workdir = f"/{project_name}/oce_uat/"
        elif environment == "DEV" or environment == "PROD":
            workdir = f"/{project_name}/oce/"
        else:
            return False
    elif process_type == "SLCE":
        if environment == "FVT":
            workdir = f"/{project_name}/slce_fvt/"
        elif environment == "UAT":
            workdir = f"/{project_name}/slce_uat/"
        elif environment == "DEV" or environment == "PROD":
            workdir = f"/{project_name}/slce/"
        else:
            return False
    else:
        return False

    return workdir


def run_java_file(java_file_name):
    # Check that the folder name is an enviroment variable or need to be
    try:
        subprocess.check_output("java /bin/com/intraware/" + java_file_name, shell=True)
    except subprocess.CalledProcessError as err:
        print("ERROR running java file")
    return "The java file run successfully"


def read_log_file(log_file_name, environment):
    path = " "
    if environment == "FVT":
        path = "/odsvt_sp_output/"
    elif environment == "UAT":
        path = "/odsqa_sp_output/"
    elif environment == "PROD":
        path = "/odsd_sp_output/"
    try:
        with open(path + log_file_name) as file:
            log_content = file.read()
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", log_file_name)
        return False
    return str(log_content)


def read_query_from_resources(file_name):
    path = f"./resources/{file_name}.sql"
    query = None
    try:
        file = open(path, encoding="utf-8")
        query = file.read()
    except OSError:
        logger.error("File from %s not found", path)
    else:
        file.close()

    return query
