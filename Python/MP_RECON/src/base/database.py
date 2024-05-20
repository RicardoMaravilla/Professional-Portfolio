"""
Name: database.py
Description: Methods to connect to a database
Created by: Álvaro González <alvaro.glez@ibm.com>
Created Date: 2020/12/22
Modification:
    date        owner           description
    2021/04/05  Alvaro Gonzalez Add parameter to use a specific user when connect to database
"""
# *- encoding = utf-8 -*

import base.constants as constants
import ibm_db
import os
import logging
import base.secrets as secrets
from base.secrets_cli import SecretsCli

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

logger = logging.getLogger()

secrets_cli = SecretsCli()


def get_db_info():
    """
    Method for get database information, read values from the environment
    variables and returns a tuple including database name, hostname and port.
    """
    database = os.environ.get(constants.DATABASE_NAME)
    hostname = os.environ.get(constants.DATABASE_HOSTNAME)
    port = os.environ.get(constants.DATABASE_PORT)

    database_info = database, hostname, port

    return database_info


def open_db_conn(environment, username=None):
    """
    Method for create an db2 connection, receives an environment
    and returns a db2 connection object or False.

    Parameters:
    - environment (str): environment to open database connection,
    - username (str) [optional]: specific username to open database connection
    """

    database, hostname, port = get_db_info()

    if not username:
        username = secrets.get_username(environment)

    db_info = secrets_cli.get_db_secrets(environment, username)

    logger.info("Retrieve credentials from Secrets Manager CLI")
    password = db_info[1]

    logger.info("Create database conn string")
    conn_str = (
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"HOSTNAME={hostname};"
        f"PORT={port};"
        f"SECURITY=SSL;"
        f"SSLClientKeystoredb={constants.SSL_CLIENT_KEYSTORE_DB};"
        f"SSLClientKeystash={constants.SSL_CLIENT_KEYSTASH};"
    )

    logger.info("Try connection to database: %s", database)
    try:
        conn = ibm_db.connect(conn_str, "", "")
    except Exception:
        logger.exception(
            "Database connection cannot be done: %s", str(ibm_db.conn_errormsg())
        )
        return False

    logger.info("Database connection succeeded.")

    return conn


def close_db_conn(conn):
    """
    Method for close an db2 connection, receives a db2 connection
    and returns a True or False.
    """
    close = ibm_db.close(conn)

    if close:
        return True

    logger.error("Error trying to close the connection")
    return False
