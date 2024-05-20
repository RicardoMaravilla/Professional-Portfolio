"""
Name: constants.py
Description: Constants used by files in cdb_oper/bin path
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/01/14
Modification:
    date        owner       description
    2022/04/29  Ric Marav   Copy from /Linux/ods/bin/ to the db2redis_dataload proyect
"""
# *- encoding = utf-8 -*

# used by database.py

DATABASE_ENVIRONMENT = "DATABASE_ENVIRONMENT"
DATABASE_NAME = "DATABASE_NAME"
DATABASE_HOSTNAME = "DATABASE_HOSTNAME"
DATABASE_PORT = "DATABASE_PORT"
SSL_CLIENT_KEYSTORE_DB = "./keystore/db2_ssl_keydb.kdb"
SSL_CLIENT_KEYSTASH = "./keystore/db2_ssl_keydb.sth"

# used by secrets.py

VAULT_ODS_PATH = "generic/project/ods/application"
VAULT_TEST_PATH = "generic/project/ods/test"
ROLE_ID_FILE = "ROLE_ID"
SECRET_ID_FILE = "SECRET_ID"
VAULT_ADDR = "vserv-us.sos.ibm.com"
VAULT_PORT = 8200

# used by utils.py

SMTP_SERVER = "relay.us.ibm.com:25"

# used by redis_dataload.py

REDIS_HOSTNAME = "REDIS_HOSTNAME"
REDIS_PORT = "REDIS_PORT"
REDIS_SWOPRADS = "REDIS_SWOPRADS"
REDIS_CERTFILE = "REDIS_CERTFILE"
REDIS_URL = "REDIS_URL"
REDIS_USER = "REDIS_USER"
