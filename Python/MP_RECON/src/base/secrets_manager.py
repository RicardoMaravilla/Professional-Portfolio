"""
Name: secrets_manager.py
Description: Get secrets from Secrets Manager (IBM CLOUD)
The Secrets Name's had to be different ALWAYS.
Created by: Ricardo Maravilla ricardo_maravilla@ibm.comm>
Created Date: 2023/02/22
Modification:
    date        owner           description
    22/09/23    Ricardo Mar     Change to version 2.0
"""

# *- encoding = utf-8 -*

import logging
from getpass import getuser

from ibm_cloud_sdk_core.authenticators.iam_authenticator import IAMAuthenticator

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

logger = logging.getLogger()


class SecretsManager:
    """
    Version 2.0
    SecretsManager ODS class. This class process the secrets
    request with only two methods.

    get_secret: that takes the name or the id and returns the secret
    get_secret_with_type: that takes the id and the type of the secret
    to return the secret.

    You need to instantiate this class and use the two methods
    described above.

    If you want to add another secret type, you only have to create his method
    and added into the method __determine_secret. No other modifications are
    allowed. (OPEN/CLOSED DESIGN PRINCIPLE)

    If you want to test all the class you have to change the level of the logging
    from logging.INFO to logging.DEBUG.

    Args:
        url: URL to connect to the secrets server in IBM Cloud
        apikey: Apikey to connect to the secrets server in IBM Cloud

    Returns:
        The secret in string format.
        username_password -> password of the username
        kv -> the first key in the payload
        arbitrary -> the text of the payload

    Raises:
        NameError: Raises an exception if the secret does not exist.
    """

    def __init__(self, url: str, apikey: str):
        self.url = url
        self.apikey = apikey
        self.secrets_manager = ""
        self.__connect()
        self.__set_url()

    def __connect(self):
        self.secrets_manager = SecretsManagerV2(
            authenticator=IAMAuthenticator(apikey=self.apikey)
        )

    def __set_url(self):
        self.secrets_manager.set_service_url(self.url)

    # ------------ General Methods --------------------------------

    def __determinate_secret(self, secret_type: str, response, key: str = None):
        """
        Determinate the kind of secret that we want to return.
        Here is where you can add more secret type if you need to.
        Single Responsibility Principle.

        Args:
            secret_type: str with the secret type
            response: JSON object containing the secret
            key: str with the option (username)

        Returns:
            The secret value in string format

        Raises:
            NameError: Raises an exception if the secret doesnt exist.
        """
        if secret_type == "username_password":
            if key == "username":
                return self.__get_username_password_username(response)
            elif key == "password" or key == None:
                return self.__get_username_password_password(response)
            else:
                raise NameError("Key error: %s" % key)
        elif secret_type == "kv":
            return self.__get_kv(response)
        elif secret_type == "arbitrary":
            return self.__get_arbitrary(response)
        else:
            raise NameError("Secret type not found")

    def __get_all_secrets_list(self, secret_key: str):
        """
        This method get all the secrets in the server that
        contains id or name equal to the key in the args.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            secret_key: str with the value that we want to
            looking for

        Returns:
            Json list of secrets or None if no secrets
        """
        all_results = []
        pager = SecretsPager(
            client=self.secrets_manager,
            limit=1,
            sort="created_at",
            search=secret_key,
        )
        while pager.has_next():
            next_page = pager.get_next()
            assert next_page is not None
            all_results.extend(next_page)

        return all_results

    def __get_secret_type(self, secret_object) -> str:
        """
        This method get the type of secret in the secret object
        in the args. secret_type is the dictonary value.
        Single Responsibility Principle.

        Args:
            secret_object: Json object with the values of the
            secret

        Returns:
            str with the type of secret
        """
        return secret_object["secret_type"]

    def __check_secret(self, secret_object):
        """
        This method check if the secret are empty or not.
        Single Responsibility Principle.

        Args:
            secret_object: Json object with the values of the
            secret

        Returns:
            The same secret object

        Raises:
            NameError: Raises an exception if the secret object
            is empty.
        """
        if secret_object == None:
            raise NameError("Secret with that Name or ID not found")

        return secret_object

    def __get_secret_with_id(self, secret_id: str):
        """
        This method gets the secret object using the method of the
        library IBM Secret Manager. This method is the final step.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            secret_id: ID of the secret that we want to get

        Returns:
            Json response with the secret values
        """
        response = self.secrets_manager.get_secret(secret_id)
        return response.get_result()

    def __get_secret_with_name(self, secret_name: str, secret_type: str):
        """
        This method gets the secret object using the method of the
        library IBM Secret Manager. This method is the final step.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            secret_type: str with the type of secret
            secret_name: Name of the secret that we want to get

        Returns:
            Json response with the secret values
        """
        response = self.secrets_manager.get_secret_by_name_type(
            secret_type=secret_type,
            name=secret_name,
            secret_group_name="default",
        )

        return response.get_result()

    # ------------ Secret Types --------------------
    def __get_username_password_password(self, response) -> str:
        """
        This method gets the password of an secret with the type:
        username_password.

        Version 2 of this method.

        Args:
            response: Json response with the values of the secret

        Returns:
            str with the password of the secret
        """
        return response["password"]

    def __get_username_password_username(self, response) -> str:
        """
        This method gets the useername of an secret with the type:
        username_password.

        Version 2 of this method.

        Args:
            response: Json response with the values of the secret

        Returns:
            str with the username of the secret
        """
        return response["username"]

    def __get_kv(self, response) -> str:
        """
        This method gets the value of the firts key in the secret.
        Also can get more values of the keys but is not required.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            response: Json response with the values of the secret

        Returns:
            str with the value of the firts key in the secret
        """
        return list(response["data"].values())[0]

    def __get_arbitrary(self, response) -> str:
        """
        This method gets the text of the secret.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            response: Json response with the values of the secret

        Returns:
            str with the text of the secret
        """
        return response["payload"]

    # ------------ ID BLOCK -----------
    def __secret_with_id(self, secret_id: str, key: str = None):
        """
        This method execute the steps to get the secret by
        id case. This is the Single Responsibility Principle collector.
        Single Responsibility Principle.

        Args:
            secret_id: str with the id of the secret
            key: str with the username

        Returns:
            str with the secret
        """
        response = self.__get_secret_with_id(secret_id)
        secret_type = self.__get_secret_type(response)
        secret = self.__determinate_secret(secret_type, response, key)
        return secret

    # ------------- NAME BLOCK ------------
    def __lookup_secrets_by_name(self, secret_name: str, secret_list):
        """
        This method gets the object of the secret list that match
        the name.
        Single Responsibility Principle.

        Version 2 of this method.

        Args:
            secret_name: str with the name of the secret
            secret_list: Secret list with all the secret

        Returns:
            Secret object that matches the name or None if not found
        """
        for secret_object in secret_list:
            if secret_object["name"] == secret_name:
                return secret_object

        return None

    def __secret_with_name(self, secret_name: str, key: str = None):
        """
        This method execute the steps to get the secret by name case.
        This is the Single Responsibility Principle collector.
        Single Responsibility Principle.

        Args:
            secret_name: str with the name of the secret
            key: str with the username

        Returns:
            str with the secret
        """
        secret_list = self.__get_all_secrets_list(secret_name)
        secret_object = self.__lookup_secrets_by_name(secret_name, secret_list)
        if not self.__check_secret(secret_object):
            return False
        secret_type = self.__get_secret_type(secret_object)
        response = self.__get_secret_with_name(secret_name, secret_type)
        secret = self.__determinate_secret(secret_type, response, key)
        return secret

    # ------------- RETURN BLOCK ------------
    # @final
    def get_secret(
        self, secret_name: str = None, secret_id: str = None, key: str = None
    ) -> str:
        """
        This method is callable and returns the secret using the
        secret name or the secret id. Can not be modified.
        Single Responsibility Principle.
        Open/Closed Principle.

        Args:
            secret_name: str with the name of the secret
                        opcional argument
            secret_id: str with the id of the secret
                        opcional argument
            key: str with the optional (username or password)

        Returns:
            str with the secret

        Raises:
            NameError: Raises an exception if the args are None
            or the secret doesn't exist.
        """
        if secret_name == None and secret_id == None:
            raise NameError("Secret name or id not found")
        elif secret_name and secret_id:
            raise NameError("Both arguments is not valid")
        elif secret_id:
            return self.__secret_with_id(secret_id, key)
        return self.__secret_with_name(secret_name, key)
