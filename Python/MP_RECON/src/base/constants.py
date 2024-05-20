"""
Name: constants.py
Description: Constants used by files in cdb_oper/bin path
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2021/01/14
Modification:
    date        owner       description
"""
# *- encoding = utf-8 -*

# used by database.py

DATABASE_ENVIRONMENT = "DATABASE_ENVIRONMENT"
DATABASE_NAME = "DATABASE_NAME"
DATABASE_HOSTNAME = "DATABASE_HOSTNAME"
DATABASE_PORT = "DATABASE_PORT"
#SSL_CLIENT_KEYSTORE_DB = "/ods/keys/db2_ssl_keydb.kdb"
#SSL_CLIENT_KEYSTASH = "/ods/keys/db2_ssl_keydb.sth"
SSL_CLIENT_KEYSTORE_DB = "./keystore/db2_ssl_keydb.kdb"
SSL_CLIENT_KEYSTASH = "./keystore/db2_ssl_keydb.sth"

# used by secrets.py

VAULT_ODS_PATH = "generic/project/ods/application"
VAULT_TEST_PATH = "generic/project/ods/test"
ROLE_ID_FILE = "/ods/secrets/role-id"
SECRET_ID_FILE = "/ods/secrets/secret-id"
VAULT_ADDR = "vserv-us.sos.ibm.com"
VAULT_PORT = 8200

# used by secrets_manager.py
SM_URL = "SM_URL"
SM_APIKEY = "SM_APIKEY"

# used by utils.py

SMTP_SERVER = "relay.us.ibm.com:25"
