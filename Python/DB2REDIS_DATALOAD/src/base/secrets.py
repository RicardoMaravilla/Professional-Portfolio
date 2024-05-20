"""
Name: secrets.py
Description: Get secrets from Vault
Created by: Alvaro Gonzalez <alvaro.glez@ibm.com>
Created Date: 2020/12/22
Modification:
    date        owner           description
    2021/04/05  Alvaro Gonzalez Add parameter to use a specific user when retrieving password
    2022/04/29  Ric Marav   Copy from /Linux/ods/bin/ to the db2redis_dataload proyect
"""

# *- encoding = utf-8 -*

import logging
import os
import base64
import hvac
import base.constants as constants
from getpass import getuser

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

logger = logging.getLogger()

client = False


def get_vault_client():
    """
    Method for get an client connected with Hashicorp Vault. If connection has not been done
    it creates a new one or validates if the current session has not expired.
    """
    global client

    if client and client.is_authenticated():
        return client

    client = auth_to_vault()
    return client


def auth_to_vault():
    """
    Method for authenticate client with Vault
    """
    try:
        client = hvac.Client()  # this method reads env variable VAULT_ADDR
    except hvac.exceptions.VaultError as exc:
        logger.error("Unable to create a HVAC Client.")
        return False

    role_id = get_role_id()
    secret_id = get_secret_id()

    try:
        resp = hvac.api.auth_methods.approle.AppRole(client).login(role_id, secret_id)
    except hvac.exceptions.VaultError as exc:
        logger.error("Unable to authenticate the HVAC Client.")
        return False

    if resp and resp["auth"] and resp["auth"]["client_token"]:
        client.token = resp["auth"]["client_token"]
        return client

    logger.error("HVAC could not retrieve token")
    return False


def get_role_id():
    """
    Method for get Role ID from filesystem
    """
    with open(os.environ.get(constants.ROLE_ID_FILE), "r") as file:
        role_id_encode = file.readlines()

    role_id_encode = role_id_encode[0]
    role_id = decode_b64(role_id_encode)

    return role_id


def get_secret_id():
    """
    Method for get Secret ID from filesystem
    """
    with open(os.environ.get(constants.SECRET_ID_FILE), "r") as file:
        secret_id_encode = file.readlines()

    secret_id_encode = secret_id_encode[0]
    secret_id = decode_b64(secret_id_encode)

    return secret_id


def get_username(environment):
    """
    Method for return database username got from Vault
    """
    if environment:
        username = "odsoper"  # Remove when deploy to production

    return username


def get_password(environment, username=None):
    """
    Method for return database password got from Vault
    """
    client = get_vault_client()
    if environment == "FVT" or environment == "UAT":
        environment = "TEST"

    if not username:
        username = get_username(environment)

    path = f"{constants.VAULT_ODS_PATH}/{environment}/{username}"

    try:
        response = client.read(path)
    except hvac.exceptions.VaultError as exc:
        logger.error("Unable to retrieve secrets from HVAC Client.")
        return False

    password = response["data"]["password"]
    return password


def decode_b64(data_encode):
    """
    Method for decode data in base64 to ascii
    """
    data_encode_bytes = data_encode.encode("ascii")
    data_decode_bytes = base64.b64decode(data_encode_bytes + b"==")
    data = data_decode_bytes.decode("ascii")
    data = data.strip("\n")
    return data


def get_sm_url():
    """
    Method for get Secrets Manager URL from filesystem
    """
    with open(os.environ.get(constants.SM_URL), "r") as file:
        sm_url_encode = file.readlines()

    sm_url_encode = sm_url_encode[0]
    sm_url = decode_b64(sm_url_encode)

    return sm_url


def get_sm_apikey():
    """
    Method for get Secrets Manager APIKEY from filesystem
    """
    with open(os.environ.get(constants.SM_APIKEY), "r") as file:
        sm_apikey_encode = file.readlines()

    sm_apikey_encode = sm_apikey_encode[0]
    sm_apikey = decode_b64(sm_apikey_encode)

    return sm_apikey
