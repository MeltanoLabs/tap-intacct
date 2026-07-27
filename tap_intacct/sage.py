"""API Base class with util functions"""  # ruff: ignore[missing-terminal-punctuation]

import datetime as dt
import http
import json
import logging
import re
import uuid
from http.client import RemoteDisconnected

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
from tap_intacct.streams import IntacctStream


class PleaseTryAgainLaterError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


from .const import INTACCT_OBJECTS  # ruff: ignore[module-import-not-at-top-of-file]

logger = logging.getLogger(__name__)


class InvalidXmlResponse(Exception):  # ruff: ignore[error-suffix-on-exception-name, undocumented-public-class]
    pass


class BadGatewayError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


class OfflineServiceError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


class RateLimitError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


IGNORE_FIELDS = ["PASSWORD"]


class SageIntacctSDK:
    """The base class for all API classes."""

    def __init__(  # ruff: ignore[missing-return-type-special-method, undocumented-public-init, too-many-arguments, too-many-positional-arguments]
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

    def _set_session_id(self, user_id: str, company_id: str, user_password: str):  # ruff: ignore[missing-return-type-private-function]
        """Sets the session id for APIs"""  # ruff: ignore[docstring-missing-exception, missing-terminal-punctuation]
        timestamp = dt.datetime.now()  # ruff: ignore[call-datetime-now-without-tzinfo]
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
            raise SageIntacctSDKError("Error: {0}".format(response["errormessage"]))  # ruff: ignore[dot-format-in-exception, format-literals]

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
    def _post_request(self, dict_body: dict, api_url: str) -> dict:  # ruff: ignore[complex-structure, too-many-branches, too-many-statements]
        """Create a HTTP post request.

        Parameters:
            dict_body (dict): HTTP POST body data for the wanted API.
            api_url (str): Url for the wanted API.

        Returns:
            A response from the request (dict).
        """  # ruff: ignore[docstring-missing-exception]
        api_headers = {"content-type": "application/xml"}
        api_headers.update(self.__headers)
        body = xmltodict.unparse(dict_body)
        patterns = [
            (r"<senderid>[^<]+</senderid>", "<senderid>[REDACTED]</senderid>"),
            (r"<password>[^<]+</password>", "<password>[REDACTED]</password>"),
            (r"<sessionid>[^<]+</sessionid>", "<sessionid>[REDACTED]</sessionid>"),
        ]

        # Apply each replacement pattern
        redacted_body = body
        for pattern, replacement in patterns:
            redacted_body = re.sub(pattern, replacement, redacted_body)
        logger.info(f"request to {api_url} with body {redacted_body}")  # ruff: ignore[logging-f-string]
        response = requests.post(api_url, headers=api_headers, data=body)  # ruff: ignore[request-without-timeout]

        logger.info(
            f"request to {api_url} response {response.text}, statuscode {response.status_code}"  # ruff: ignore[logging-f-string]
        )
        try:
            parsed_xml = xmltodict.parse(response.text)
            parsed_response = json.loads(json.dumps(parsed_xml))
        except:  # ruff: ignore[bare-except]
            if response.status_code == http.HTTPStatus.BAD_GATEWAY:  # 502
                raise BadGatewayError(  # ruff: ignore[raise-without-from-inside-except, raise-vanilla-args]
                    f"Response status code: {response.status_code}, response: {response.text}"  # ruff: ignore[f-string-in-exception]
                )
            if response.status_code == http.HTTPStatus.SERVICE_UNAVAILABLE:  # 503
                raise OfflineServiceError(  # ruff: ignore[raise-without-from-inside-except, raise-vanilla-args]
                    f"Response status code: {response.status_code}, response: {response.text}"  # ruff: ignore[f-string-in-exception]
                )
            if response.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:  # 429
                raise RateLimitError(  # ruff: ignore[raise-without-from-inside-except, raise-vanilla-args]
                    f"Response status code: {response.status_code}, response: {response.text}"  # ruff: ignore[f-string-in-exception]
                )
            raise InvalidXmlResponse(  # ruff: ignore[raise-without-from-inside-except, raise-vanilla-args]
                f"Response status code: {response.status_code}, response: {response.text}"  # ruff: ignore[f-string-in-exception]
            )

        if response.status_code == http.HTTPStatus.OK:  # 200
            if parsed_response["response"]["control"]["status"] == "success":
                api_response = parsed_response["response"]["operation"]

            if parsed_response["response"]["control"]["status"] == "failure":
                exception_msg = IntacctStream.decode_support_id(
                    parsed_response["response"]["errormessage"]
                )
                raise WrongParamsError(  # ruff: ignore[raise-vanilla-args]
                    "Some of the parameters are wrong",  # ruff: ignore[raw-string-in-exception]
                    exception_msg,
                )

            if api_response["authentication"]["status"] == "failure":
                raise InvalidTokenError(  # ruff: ignore[raise-vanilla-args]
                    "Invalid token / Incorrect credentials",  # ruff: ignore[raw-string-in-exception]
                    api_response["errormessage"],
                )

            if api_response["result"]["status"] == "success":
                return api_response

            logger.error(f"Intacct error response: {api_response}")  # ruff: ignore[logging-f-string]
            error = api_response.get("result", {}).get("errormessage", {}).get("error", {})
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
                and "There was an error processing the request" in desc_2  # type: ignore[operator]
                and dict_body["request"]["operation"]["content"]["function"]["query"]["object"]
                == "AUDITHISTORY"
            ):
                return {"result": "skip_and_paginate"}

        exception_msg = parsed_response.get("response", {}).get("errormessage", {}).get("error", {})
        correction = exception_msg.get("correction", {})  # type: ignore[union-attr]

        if response.status_code == http.HTTPStatus.BAD_REQUEST:  # 400
            if exception_msg.get("errorno") == "GW-0011":  # type: ignore[union-attr]
                raise AuthFailure(  # ruff: ignore[raise-vanilla-args]
                    f"One or more authentication values are incorrect. Response:{parsed_response}"  # ruff: ignore[f-string-in-exception]
                )
            raise InvalidRequest("Invalid request", parsed_response)  # ruff: ignore[raw-string-in-exception, raise-vanilla-args]

        if response.status_code == http.HTTPStatus.UNAUTHORIZED:  # 401
            raise InvalidTokenError(  # ruff: ignore[raise-vanilla-args]
                f"Invalid token / Incorrect credentials. Response: {parsed_response}"  # ruff: ignore[f-string-in-exception]
            )

        if response.status_code == http.HTTPStatus.FORBIDDEN:  # 403
            raise NoPrivilegeError(  # ruff: ignore[raise-vanilla-args]
                f"Forbidden, the user has insufficient privilege. Response: {parsed_response}"  # ruff: ignore[f-string-in-exception]
            )

        if response.status_code == http.HTTPStatus.NOT_FOUND:  # 404
            raise NotFoundItemError(  # ruff: ignore[raise-vanilla-args]
                f"Not found item with ID. Response: {parsed_response}"  # ruff: ignore[f-string-in-exception]
            )

        if response.status_code == http.HTTPStatus.INTERNAL_SERVER_ERROR:  # 500
            raise InternalServerError(  # ruff: ignore[raise-vanilla-args]
                f"Internal server error. Response: {parsed_response}"  # ruff: ignore[f-string-in-exception]
            )

        if response.status_code == 498:  # ruff: ignore[magic-value-comparison]
            raise ExpiredTokenError(  # ruff: ignore[raise-vanilla-args]
                f"Expired token, try to refresh it. Response: {parsed_response}"  # ruff: ignore[f-string-in-exception]
            )

        if correction and "Please Try Again Later" in correction:
            raise PleaseTryAgainLaterError(parsed_response)

        raise SageIntacctSDKError(f"Error: {parsed_response}")  # ruff: ignore[f-string-in-exception, raise-vanilla-args]

    def format_and_send_request(self, data: dict) -> list | dict:
        """Format data accordingly to convert them to xml.

        Parameters:
            data (dict): HTTP POST body data for the wanted API.

        Returns:
            A response from the _post_request (dict).
        """
        key = next(iter(data))
        object_type = data[key]["object"]  # ruff: ignore[unused-variable]
        timestamp = dt.datetime.now()  # ruff: ignore[call-datetime-now-without-tzinfo]

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
                    "content": {"function": {"@controlid": str(uuid.uuid4()), key: data[key]}},
                },
            }
        }
        # with singer.metrics.http_request_timer(endpoint=object_type):
        response = self._post_request(dict_body, self.__api_url)
        return response["result"]

    def get_fields_data_using_schema_name(self, object_type: str):  # ruff: ignore[missing-return-type-undocumented-public-function]
        """Fetch fields data for a given object.

        Takes the schema name through the API call. This function helps query
        via the api for any given schema name.

        Returns:
            List of dict in object_type schema.
        """
        intacct_object_type = INTACCT_OBJECTS[object_type]

        # First get the count of object that will be synchronized.
        get_fields = {"lookup": {"object": intacct_object_type}}

        response = self.format_and_send_request(get_fields)
        return response  # ruff: ignore[unnecessary-assign]

    def load_schema_from_api(self, stream: str):  # ruff: ignore[missing-return-type-undocumented-public-function]
        """Get the API schema.

        Function to load schema data via an api call for each INTACCT Object to get the fields
        list for each schema name dynamically.

        Args:
            stream: The stream name.

        Returns:
            The JSON Schema as a dictionary.

        """
        properties: list[th.Property] = []
        required_list = ["RECORDNO", "WHENMODIFIED"]
        fields_data_response = self.get_fields_data_using_schema_name(object_type=stream)
        fields_data_list = fields_data_response["data"]["Type"]["Fields"]["Field"]
        for rec in fields_data_list:
            if rec["ID"] in IGNORE_FIELDS:
                continue
            if rec["DATATYPE"] in {"PERCENT", "DECIMAL"}:
                type_data_type = th.NumberType
            elif rec["DATATYPE"] == "BOOLEAN":
                type_data_type = th.BooleanType
            elif rec["DATATYPE"] in {"DATE", "TIMESTAMP"}:
                type_data_type = th.DateTimeType
            else:
                type_data_type = th.StringType
            properties.append(
                th.Property(rec["ID"], type_data_type, required=(rec["ID"] in required_list))
            )
        return th.PropertiesList(*properties).to_dict()


def get_client(  # ruff: ignore[too-many-arguments]
    *,
    api_url: str,
    company_id: str,
    sender_id: str,
    sender_password: str,
    user_id: str,
    user_password: str,
    headers: dict,
) -> SageIntacctSDK:
    """Initializes and returns a SageIntacctSDK object.

    Returns:
        The SageIntacctSDK object.
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
