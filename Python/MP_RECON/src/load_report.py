"""
Name: load_report.py
Description: Sends the load report
Created by: Ricardo Maravilla <ricardo.maravilla@ibm.com>
Created Date: 2020/12/22
Modification:
    date        owner           description
"""

# *- encoding = utf-8 -*

import base.constants as constants
import base.database as database
import base.secrets as secrets
import base.utils as utils
import argparse
import ibm_db
import os
import logging
import sys
from time import strftime, localtime, time

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

logger = logging.getLogger()

SP_REGEX = r"^\w+[.]dp_\w+$"
PROCESS_NAME = "Load Report Process"
CONFIG_FILES = "./cfg/"
LOGS_FILES = "./logs/"
RESOURCES_FILES = "./resources/"
EXECUTION_TYPE_DEFAULT = "default"
SCRIPT_DESCRIPTION = (
    "This script will do the report for the records table. \n"
)

def read_log(log_name):
    '''
        This method will read a log file.
        Parameters
            - log_name: The name of the log file
        Returns
            On success, log_content (str): The content of the log file
            On failure, False
    '''
    try:
        with open(LOGS_FILES + log_name) as file:
            log_content = file.read()
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", log_name)
        return False
    return str(log_content)

def read_cfg_folder_full():
    '''
        This method will read the configuration files in the folder.
        Returns
            On success, cfg_files (list): The list of the configuration files
            On failure, False
    '''
    cfg_files = []
    for root, dirs, files in os.walk(CONFIG_FILES):
        for file in files:
            if file.endswith(".cfg"):
                cfg_files.append(os.path.join(root, file))
        if not cfg_files:
            logger.error("No configuration files found.")
            return False
    logger.info("Configuration files found: %s", len(cfg_files))
    return cfg_files

def read_config_file(config_file_name):
    '''
        This method read the config file that contains the list of the querys.
        Parameters
            - config_file_name: The name of the file that contains the list of Stored Procedures
        Returns
            On success, query_list (list): The list of the queries
            On failure, False
    '''
    try:
        with open(config_file_name) as file:
            config_file = file.read().splitlines()
            query_list = []
            for line in config_file:
                query_list.append(line.split("|")[0])
            logger.info("The config file %s has been read.", config_file_name.split("/")[-1])
            return query_list
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", config_file_name)
        return False

def select_query(conn, table, date):
    '''
        This method create and execute a query to get the count per part_num at the day.
        Must be 2 per part_num or the record have a mismatch
        Parameters
            - conn: The ibmdb connection
            - table: The name of the table to query
            - date: The today date
        Returns
            On success, record_per_day (list): The list of the record count by day
            On failure, False
    '''
    select_statement = "SELECT part_num, count(*) FROM "+ table + " WHERE ODS_MOD_DATE > '" + date + "' GROUP BY part_num"
    
    try:
        record_per_day = ibm_db.exec_immediate(conn,select_statement)
    except Exception:
        logger.error("Query could not be executed: %s", ibm_db.stmt_errormsg())
        return False

    if not record_per_day:
        logger.warning("Empty result for the table %s", table)
        return False

    logger.info("SELECT query for the table %s has been executed", table)

    return record_per_day

def pair_records(record_list):
    '''
        This method check if the records have the 2 per day and match.
        Parameters
            - record_list: The list of the records to be paired
        Returns
            On success, record_mismatch_list (list): The list of the mismatch records
            On failure, False
    '''
    record_mismatch_list = []
    data = ibm_db.fetch_both(record_list)
    while data != False:
        if data['2'] != 2:
            record_mismatch_list.append(data['PART_NUM'])
        data = ibm_db.fetch_both(record_list)


    logger.info("Records that mismatch: %s", record_mismatch_list)
    return record_mismatch_list

def create_msg_email(record_mismatch_list):
    '''
        This method generate the msg for the report email.
        Parameters
            - record_mismatch_list: The list of the records with mismatch
        Returns
            On success, message (str): Str with the message report
            On failure, False
    '''
    message = "These records have a mismatch this day, you need to check the ffxt1 tables and the wwwpp2 to find the issue." + "\n" + "Part_Num list: "+ "\n"
    for record in record_mismatch_list:
        message = message  + "- " + record + "\n"
    message = message + "\n" + "Tables List: " + "\n"
    message = message + "- WWPP2.CNTRY_PRICE_TIERD_DRCT" + "\n"
    message = message + "- WWPP2.CNTRY_PRICE_DRCT" + "\n"
    message = message + "- WWPP2.PROD_PUBLSHD" + "\n"
    message = message + "- WWPP2.WWIDE_FNSHD_GOOD_SAP" + "\n"
    message = message + "- FFXT1.PRICING_TIERD_ECOM_SCW" + "\n"
    message = message + "- FFXT1.SW_PROD_SCW" + "\n"

    logger.info("Email Created")
    return message

def full_load(conn, date):
    '''
        This method runs the full load standard.
        Parameters
            - conn: The ibmdb database connection
            - date: The todays date
        Returns
            On success, email(str): Str with the recon email.
            On failure, False
    '''
    cfg_files = read_cfg_folder_full()
    for cfg_file in cfg_files:
        table_list = read_config_file(cfg_file)
        if not table_list:
            logger.error("Error reading configs file.")
            return False
        for table in table_list:
            result = select_query(conn, table, date)
        if not result:
            logger.error("Error executing the select query")
            return False
        record_mismatch_list = pair_records(result)
        if not record_mismatch_list:
            logger.warning("No records was found today")
            return "No records were found today."
        email = create_msg_email(record_mismatch_list)

    return email


def main():
    '''
        This is the main method for the class.
        Returns
            On success, True
            On failure, False
    '''
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
    parser.add_argument("-f", "--full", action="store_true", help="Full Execution for Dataload.")
    args = parser.parse_args()
    if args.full:
        execution_type = EXECUTION_TYPE_DEFAULT
    else:
        parser.print_help()
        sys.exit(-1)

    start_time = localtime()
    process_time = strftime('%Y-%m-%d', start_time)
    logger.info("Starting full load")

    log_name = "logfile_" + str(process_time) + ".log"
    handler = logging.FileHandler(LOGS_FILES + log_name)
    logger.addHandler(handler)

    logger.info("%s process", PROCESS_NAME)
    logger.info("------------------------------------------------")

    logger.info("Set up Environment")
    environment = os.environ.get("DATABASE_ENVIRONMENT")

    logger.info("Open DB2 connection")
    conn = database.open_db_conn(environment,os.environ.get("DATABASE_USERNAME"))
    recipient = os.environ.get("EMAIL_NOTIFICATION_RECIPIENT")
    if not conn:
        subject = (f"FAILURE: {PROCESS_NAME} {environment}"
                   f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}")
        message = "Database connection could not be established." + "\n" + "Please check the log:" + "\n"
        message = message + read_log(log_name)
        logger.error("Database connection could not be established.")
        utils.send_mail(recipient, subject, message)
        sys.exit(-1)

    if execution_type == EXECUTION_TYPE_DEFAULT:
        logger.info("Execution type: DEFAUL - FULL")
        result = full_load(conn, process_time)
        if not result:
            subject = (f"FAILURE: {PROCESS_NAME} {environment}"
                       f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}")
            message = "Error executing full load." + "\n" + "Please check the log:" + "\n"
            message = message + read_log(log_name)
            logger.error("Error executing full load.")
            utils.send_mail(recipient, subject, message)
            sys.exit(-1)

    logger.info("Close DB2 connection")
    if not database.close_db_conn(conn):
        subject = (f"FAILURE: {PROCESS_NAME} {environment}"
                   f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}")
        message = "Database connection could not be closed." + "\n" + "Please check the log:" + "\n"
        message = message + read_log(log_name)
        logger.error("Database connection could not be closed.")
        utils.send_mail(recipient, subject, message)
        sys.exit(-1) 


    subject = (f"SUCCESS: {PROCESS_NAME} {environment}"
               f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}")
    message = "Load Report process finished successfully." + "\n"
    logger.info("Load Report app ends successfully.")
    message = message + "\n" + "Load Report:" + "\n" + "\n" + result
    utils.send_mail(recipient, subject, message)
    exit(0)

if __name__ == "__main__":
    main()