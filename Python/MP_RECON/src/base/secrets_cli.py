"""
Name: secrets_cli.py
Description: Get secrets from Secrets CLI (Data tech)
The Secrets Name's had to be different ALWAYS.
Created by: Ricardo Maravilla ricardo_maravilla@ibm.comm>
Created Date: 2023/09/22
Modification:
    date        owner           description
"""

# *- encoding = utf-8 -*

from base.secrets import decode_b64
from getpass import getuser
import os


class SecretsCli:
    """
    Class to get the secrets from a local var in the server.
    That local var obtain the secrets from the CLI to ibm cloud.

    If you need to implement more secrets contact the datatech team.

    Returns:
        The secret value in str
        Or
        The DB values in Baikal/Sulu

    Raises:
        NameError: Raises an exception if the secret does not exist.
    """

    def get_secret_by_file(self, secret_filename: str) -> str:
        """
        This method gets the secret specified in the name
        from ibm cloud in a file.
        The secrets must be created my datatech and the name
        is the local env name.
        Single Responsibility Principle.

        Returns:
            str with the secret value
        """
        reader = open(secret_filename, "r")
        return decode_b64(reader.read())

    def get_secret(self, secret_name: str) -> str:
        """
        This method gets the secret specified in the name
        from ibm cloud in a local env.
        The secrets must be created my datatech and the name
        is the local env name.
        Single Responsibility Principle.

        Returns:
            str with the secret value
        """
        secret_value = os.environ.get(secret_name)
        return secret_value

    def get_db_secrets(self, env: str, user: str) -> list:
        """
        This method gets the db odsoper information from ibm cloud.
        Single Responsibility Principle.

        Parameters:
            env: str with the name of the environment
            user: str with the name of the user

        Returns:
            list with odsoper values
        """
        if env == "DEV":
            env_db = "DEV"
        elif env == "FVT":
            env_db = "VT"
        elif env == "UAT":
            env_db = "QA"
        else:
            env_db = "PROD"

        db_username = self.get_secret(user.upper() + "_USERNAME_" + env_db)
        db_passwd = self.get_secret(user.upper() + "_PASSWD_" + env_db)

        return [(db_username), (db_passwd)]
