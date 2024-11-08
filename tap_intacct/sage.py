"""
API Base class with util functions
"""

import datetime as dt
import json
import logging
import uuid
from http.client import RemoteDisconnected
from typing import Union

import backoff
import requests
import xmltodict
from singer_sdk import typing as th

from tap_intacct.exceptions import (
    AuthFailure,
    ExpiredTokenError,
    InternalServerError,
    InvalidRequest,
    InvalidTokenError,
    NoPrivilegeError,
    NotFoundItemError,
    SageIntacctSDKError,
    WrongParamsError,
)


class PleaseTryAgainLaterError(Exception):
    pass


from .const import GET_BY_DATE_FIELD, INTACCT_OBJECTS, KEY_PROPERTIES, REP_KEYS

logger = logging.getLogger(__name__)


class InvalidXmlResponse(Exception):
    pass


class BadGatewayError(Exception):
    pass


class OfflineServiceError(Exception):
    pass


class RateLimitError(Exception):
    pass


IGNORE_FIELDS = ["PASSWORD"]


class SageIntacctSDK:
    """The base class for all API classes."""

    def __init__(
        self,
        api_url: str,
        company_id: str,
        sender_id: str,
        sender_password: str,
        user_id: str,
        user_password: str,
        headers: dict,
    ):
        self.__api_url = api_url
        self.__company_id = company_id
        self.__sender_id = sender_id
        self.__sender_password = sender_password
        self.__user_id = user_id
        self.__user_password = user_password
        self.__headers = headers

        """
        Initialize connection to Sage Intacct
        :param sender_id: Sage Intacct sender id
        :param sender_password: Sage Intacct sender password
        :param user_id: Sage Intacct user id
        :param company_id: Sage Intacct company id
        :param user_password: Sage Intacct user password
        """
        # Initializing variables
        self._set_session_id(
            user_id=self.__user_id,
            company_id=self.__company_id,
            user_password=self.__user_password,
        )

    def _set_session_id(self, user_id: str, company_id: str, user_password: str):
        """
        Sets the session id for APIs
        """

        timestamp = dt.datetime.now()
        dict_body = {
            "request": {
                "control": {
                    "senderid": self.__sender_id,
                    "password": self.__sender_password,
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {
                        "login": {
                            "userid": user_id,
                            "companyid": company_id,
                            "password": user_password,
                        }
                    },
                    "content": {
                        "function": {
                            "@controlid": str(uuid.uuid4()),
                            "getAPISession": None,
                        }
                    },
                },
            }
        }

        response = self._post_request(dict_body, self.__api_url)

        if response["authentication"]["status"] == "success":
            session_details = response["result"]["data"]["api"]
            self.__api_url = session_details["endpoint"]
            self.__session_id = session_details["sessionid"]

        else:
            raise SageIntacctSDKError("Error: {0}".format(response["errormessage"]))

    @backoff.on_exception(
        backoff.expo,
        (
            BadGatewayError,
            OfflineServiceError,
            ConnectionError,
            ConnectionResetError,
            requests.exceptions.ConnectionError,
            InternalServerError,
            RateLimitError,
            RemoteDisconnected,
        ),
        max_tries=8,
        factor=3,
    )
    # @singer.utils.ratelimit(10, 1)
    def _post_request(self, dict_body: dict, api_url: str) -> dict:
        """
        Create a HTTP post request.

        Parameters:
            dict_body (dict): HTTP POST body data for the wanted API.
            api_url (str): Url for the wanted API.

        Returns:
            A response from the request (dict).
        """

        api_headers = {"content-type": "application/xml"}
        api_headers.update(self.__headers)
        body = xmltodict.unparse(dict_body)
        logger.info(f"request to {api_url} with body {body}")
        response = requests.post(api_url, headers=api_headers, data=body)

        logger.info(
            f"request to {api_url} response {response.text}, statuscode {response.status_code}"
        )
        try:
            parsed_xml = xmltodict.parse(response.text)
            parsed_response = json.loads(json.dumps(parsed_xml))
        except:
            if response.status_code == 502:
                raise BadGatewayError(
                    f"Response status code: {response.status_code}, response: {response.text}"
                )
            if response.status_code == 503:
                raise OfflineServiceError(
                    f"Response status code: {response.status_code}, response: {response.text}"
                )
            if response.status_code == 429:
                raise RateLimitError(
                    f"Response status code: {response.status_code}, response: {response.text}"
                )
            raise InvalidXmlResponse(
                f"Response status code: {response.status_code}, response: {response.text}"
            )

        if response.status_code == 200:
            if parsed_response["response"]["control"]["status"] == "success":
                api_response = parsed_response["response"]["operation"]

            if parsed_response["response"]["control"]["status"] == "failure":
                exception_msg = self.decode_support_id(
                    parsed_response["response"]["errormessage"]
                )
                raise WrongParamsError(
                    "Some of the parameters are wrong", exception_msg
                )

            if api_response["authentication"]["status"] == "failure":
                raise InvalidTokenError(
                    "Invalid token / Incorrect credentials",
                    api_response["errormessage"],
                )

            if api_response["result"]["status"] == "success":
                return api_response

            logger.error(f"Intacct error response: {api_response}")
            error = (
                api_response.get("result", {}).get("errormessage", {}).get("error", {})
            )
            desc_2 = (
                error.get("description2")
                if isinstance(error, dict)
                else error[0].get("description2")
                if isinstance(error, list) and error
                else ""
            )
            if (
                api_response["result"]["status"] == "failure"
                and error
                and "There was an error processing the request" in desc_2
                and dict_body["request"]["operation"]["content"]["function"]["query"][
                    "object"
                ]
                == "AUDITHISTORY"
            ):
                return {"result": "skip_and_paginate"}

        exception_msg = (
            parsed_response.get("response", {}).get("errormessage", {}).get("error", {})
        )
        correction = exception_msg.get("correction", {})

        if response.status_code == 400:
            if exception_msg.get("errorno") == "GW-0011":
                raise AuthFailure(
                    f"One or more authentication values are incorrect. Response:{parsed_response}"
                )
            raise InvalidRequest("Invalid request", parsed_response)

        if response.status_code == 401:
            raise InvalidTokenError(
                f"Invalid token / Incorrect credentials. Response: {parsed_response}"
            )

        if response.status_code == 403:
            raise NoPrivilegeError(
                f"Forbidden, the user has insufficient privilege. Response: {parsed_response}"
            )

        if response.status_code == 404:
            raise NotFoundItemError(
                f"Not found item with ID. Response: {parsed_response}"
            )

        if response.status_code == 498:
            raise ExpiredTokenError(
                f"Expired token, try to refresh it. Response: {parsed_response}"
            )

        if response.status_code == 500:
            raise InternalServerError(
                f"Internal server error. Response: {parsed_response}"
            )

        if correction and "Please Try Again Later" in correction:
            raise PleaseTryAgainLaterError(parsed_response)

        raise SageIntacctSDKError("Error: {0}".format(parsed_response))

    def format_and_send_request(self, data: dict) -> Union[list, dict]:
        """
        Format data accordingly to convert them to xml.

        Parameters:
            data (dict): HTTP POST body data for the wanted API.

        Returns:
            A response from the _post_request (dict).
        """

        key = next(iter(data))
        object_type = data[key]["object"]
        timestamp = dt.datetime.now()

        dict_body = {
            "request": {
                "control": {
                    "senderid": self.__sender_id,
                    "password": self.__sender_password,
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"sessionid": self.__session_id},
                    "content": {
                        "function": {"@controlid": str(uuid.uuid4()), key: data[key]}
                    },
                },
            }
        }
        # with singer.metrics.http_request_timer(endpoint=object_type):
        response = self._post_request(dict_body, self.__api_url)
        return response["result"]

    def get_fields_data_using_schema_name(self, object_type: str):
        """
        Function to fetch fields data for a given object by taking the schema name through
        the API call.This function helps query via the api for any given schema name
        Returns:
            List of dict in object_type schema.
        """
        intacct_object_type = INTACCT_OBJECTS[object_type]

        # First get the count of object that will be synchronized.
        get_fields = {"lookup": {"object": intacct_object_type}}

        response = self.format_and_send_request(get_fields)
        return response

    def load_schema_from_api(self, stream: str):
        """
        Function to load schema data via an api call for each INTACCT Object to get the fields list for each schema name
        dynamically
        Args:
            stream:

        Returns:
            schema_dict

        """
        properties: list[th.Property] = []
        required_list = ["RECORDNO", "WHENMODIFIED"]
        fields_data_response = self.get_fields_data_using_schema_name(
            object_type=stream
        )
        fields_data_list = fields_data_response["data"]["Type"]["Fields"]["Field"]
        for rec in fields_data_list:
            if rec["ID"] in IGNORE_FIELDS:
                continue
            if rec["DATATYPE"] in ["PERCENT", "DECIMAL"]:
                type_data_type = th.NumberType
            elif rec["DATATYPE"] == "BOOLEAN":
                type_data_type = th.BooleanType
            elif rec["DATATYPE"] in ["DATE", "TIMESTAMP"]:
                type_data_type = th.DateTimeType
            else:
                type_data_type = th.StringType
            properties.append(
                th.Property(
                    rec["ID"], type_data_type, required=(rec["ID"] in required_list)
                )
            )
        return th.PropertiesList(*properties).to_dict()


def get_client(
    *,
    api_url: str,
    company_id: str,
    sender_id: str,
    sender_password: str,
    user_id: str,
    user_password: str,
    headers: dict,
) -> SageIntacctSDK:
    """
    Initializes and returns a SageIntacctSDK object.
    """
    return SageIntacctSDK(
        api_url=api_url,
        company_id=company_id,
        sender_id=sender_id,
        sender_password=sender_password,
        user_id=user_id,
        user_password=user_password,
        headers=headers,
    )
