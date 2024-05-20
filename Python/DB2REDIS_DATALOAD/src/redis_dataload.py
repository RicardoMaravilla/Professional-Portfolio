"""
Name: redis_dataload.py
Description:
-- default run
    Start process
        for each configuration file in /cfg folder
            for each line in configuration file

                read query
                execute query in the specified database
                get results from query

                prepare redis statement to store data into a SET data structure
                connect to redis database
                pipelining statement into redis

            log how many records were sent to redis
    End process
Redis topic name:
    ods{environment}.[configuration file name]:[query file name]
Structure:
    - ./cfg/: The folder that contains the configuration files, each file contains a list of queries
    which are in the ./resources/ folder. The name of the cfg is used in the redis topic name
    - ./keystore/: The folder that contains the files for loggins and certs
    - ./logs/: The folder that contains the log files, is sended in the email
    - ./resources/: The folder that contains the sql querys files, the query file name is used
    in the redis topic name. The ./cfg files look for the sql querys in this folder.
    [query file name]|[environment]
    - ./src/: The folder that contains the python files
        - ./base/: The folder that contains the base python files for general use
        - redis_dataload.py: The python file that contains the main function
Created by: Ricardo Maravilla <alvaro.glez@ibm.com>
Created Date: 2022/04/29
Modification:
    date        owner       description
"""

# *- encoding = utf-8 -*

import base.constants as constants
import base.database as database
import base.secrets as secrets
import base.utils as utils
import argparse
import os
import logging
import redis
import sys
from time import strftime, localtime, time

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

logger = logging.getLogger()
error_list = []
SP_REGEX = r"^\w+[.]dp_\w+$"
PROCESS_NAME = "REDIS_DATALOAD"
CONFIG_FILES = "./cfg/"
LOGS_FILES = "./logs/"
RESOURCES_FILES = "./resources/"
EXECUTION_TYPE_DEFAULT = "default"
SCRIPT_DESCRIPTION = "This script will load data into a redis database.\n"


def read_cfg_folder_full():
    """
    This method will read the configuration files in the folder.
    Returns
        On success, cfg_files (list): The list of the configuration files
        On failure, False
    """
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
    """
    This method read the config file that contains the list of the querys.
    Parameters
        - config_file_name: The name of the file that contains the list of Stored Procedures
    Returns
        On success, query_list (list): The list of the queries
        On failure, False
    """
    try:
        with open(config_file_name) as file:
            config_file = file.read().splitlines()
            query_list = []
            for line in config_file:
                query_list.append(line.split("|"))
            logger.info(
                "The config file %s has been read.", config_file_name.split("/")[-1]
            )
            return query_list
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", config_file_name)
        return False


def read_query_file(query_file_name):
    """
    This method will read the query file.
    Parameters
        - query_file_name: The name of the file that contains the query
    Returns
        On success, query_statement (str): The query statement
        On failure, False
    """
    query = ""
    try:
        with open(query_file_name, "r") as query_file_name_read:
            for line in query_file_name_read:
                query += line
        logger.info("The query file %s has been read.", query_file_name.split("/")[-1])
        return query
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", query_file_name)
        return False


def set_redis_statement(query_result, redis_conn, cfg_file_name, query_file_name):
    """
    This method will prepare the redis statement.
    Parameters
        - query_result: The result of the query
        - redis_conn: The redis connection
    Returns
        On success, result (int): The result of the operation
        On failure, false
    """
    logger.info("Starting set redis statement")
    env = str(os.environ.get("DATABASE_ENVIRONMENT").lower())
    topic_name = str(
        "ods" + env + "." + str(cfg_file_name) + ":" + str(query_file_name)
    )
    redis_conn.sadd(topic_name, *query_result)
    result = redis_conn.scard(topic_name)
    if not result:
        logger.error("Error setting redis statement.")
        return False
    logger.info("Redis statement has been set with the name %s", topic_name)
    return result


def redis_connection(host, username, port, password, certfile):
    """
    This method will create a connection to redis.
    Parameters
        - host: The host of the redis database
        - port: The port of the redis database
        - password: The password of the redis database
    Returns
        On success, redis_conn (redis.Redis): The redis connection
        On failure, False
    """
    try:
        url = "rediss://" + username + ":" + password + "@" + host + ":" + port
        redis_conn = redis.StrictRedis.from_url(
            url, ssl_cert_reqs="required", ssl_ca_certs=certfile
        )
        logger.info("Redis connection has been created.")
        return redis_conn
    except redis.ConnectionError:
        logger.error("Error creating redis connection.")
        return False


def full_load(conn):
    """
    This method will load the data into redis.
    Parameters
        - conn: The connection to the database
    Returns
        On success, True
        On failure, False
    """
    logger.info("Starting full load")

    r = redis_connection(
        os.environ.get(constants.REDIS_HOSTNAME),
        os.environ.get(constants.REDIS_USER),
        os.environ.get(constants.REDIS_PORT),
        os.environ.get(constants.REDIS_SWOPRADS),
        os.environ.get(constants.REDIS_CERTFILE),
    )

    if not r:
        logger.error("Error creating redis connection.")
        return False

    cfg_files = read_cfg_folder_full()
    for cfg_file in cfg_files:
        query_list = read_config_file(cfg_file)
        if not query_list:
            logger.error("Error reading configs file.")
            return False
        for query in query_list:
            query_statement = read_query_file(RESOURCES_FILES + query[0])
            if not query_statement:
                logger.error("Error reading query file.")
                return False
            result = database.run_statement(conn, query_statement)
            if not result:
                logger.error("Error running query.")
                return False
            logger.info(
                "Query %s in Database %s with rows %s has been pulled.",
                query[0],
                query[1],
                len(result),
            )
            result_redis = set_redis_statement(
                result, r, cfg_file.split("/")[-1].split(".")[0], query[0].split(".")[2]
            )
            if not result_redis:
                logger.error("Error setting redis statement.")
                return False
            else:
                logger.info(
                    "Redis statement has been set. The result is %s", result_redis
                )

    logger.info("Full load finished.")
    return True


def read_log(log_name):
    """
    This method will read a log file.
    Parameters
        - log_name: The name of the log file
    Returns
        On success, log_content (str): The content of the log file
        On failure, False
    """
    try:
        with open(LOGS_FILES + log_name) as file:
            log_content = file.read()
    except FileNotFoundError:
        logger.error("Config File could not be read: %s", log_name)
        return False
    return str(log_content)


def main():
    """
    This is the main method for the class.
    Returns
        On success, True
        On failure, False
    """
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
    parser.add_argument(
        "-f", "--full", action="store_true", help="Full Execution for Dataload."
    )
    args = parser.parse_args()
    if args.full:
        execution_type = EXECUTION_TYPE_DEFAULT
    else:
        parser.print_help()
        sys.exit(-1)

    start_time = localtime()
    process_time = strftime("%Y-%m-%d-%H.%M.%S", start_time)

    log_name = "logfile_" + str(process_time) + ".log"
    # log_name = "logfile_redis_dataload.log"
    handler = logging.FileHandler(LOGS_FILES + log_name)
    logger.addHandler(handler)

    logger.info("%s process", PROCESS_NAME)
    logger.info("------------------------------------------------")

    logger.info("Set up Environment")
    environment = os.environ.get("DATABASE_ENVIRONMENT")

    logger.info("Open DB2 connection")
    conn = database.open_db_conn(environment, os.environ.get("DATABASE_USERNAME"))
    recipient = os.environ.get("EMAIL_NOTIFICATION_RECIPIENT")
    if not conn:
        subject = (
            f"FAILURE: {PROCESS_NAME} {environment}"
            f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}"
        )
        message = (
            "Database connection could not be established."
            + "\n"
            + "Please check the log:"
            + "\n"
        )
        message = message + read_log(log_name)
        logger.error("Database connection could not be established.")
        utils.send_mail(recipient, subject, message)
        sys.exit(-1)

    if execution_type == EXECUTION_TYPE_DEFAULT:
        logger.info("Execution type: DEFAUL - FULL")
        result = full_load(conn)
        if not result:
            subject = (
                f"FAILURE: {PROCESS_NAME} {environment}"
                f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}"
            )
            message = (
                "Error executing full load." + "\n" + "Please check the log:" + "\n"
            )
            message = message + read_log(log_name)
            logger.error("Error executing full load.")
            utils.send_mail(recipient, subject, message)
            sys.exit(-1)

    logger.info("Close DB2 connection")
    if not database.close_db_conn(conn):
        subject = (
            f"FAILURE: {PROCESS_NAME} {environment}"
            f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}"
        )
        message = (
            "Database connection could not be closed."
            + "\n"
            + "Please check the log:"
            + "\n"
        )
        message = message + read_log(log_name)
        logger.error("Database connection could not be closed.")
        utils.send_mail(recipient, subject, message)
        sys.exit(-1)

    subject = (
        f"SUCCESS: {PROCESS_NAME} {environment}"
        f" at {strftime('%Y-%m-%d-%H.%M.%S', start_time)}"
    )
    message = "DB2Redis process finished successfully." + "\n" + "Check the log:" + "\n"
    message = message + str(read_log(log_name))
    logger.info("DB2Redis process finished successfully.")
    utils.send_mail(recipient, subject, message)
    exit(0)


if __name__ == "__main__":
    main()
