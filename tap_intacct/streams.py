"""Stream type classes for tap-intacct."""

from __future__ import annotations

import http
import json
import logging
import re
import typing as t
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote

import requests
import xmltodict
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.pagination import (
    BaseAPIPaginator,
    BaseOffsetPaginator,
)
from singer_sdk.streams import RESTStream

from tap_intacct.const import GET_BY_DATE_FIELD, KEY_PROPERTIES, REP_KEYS
from tap_intacct.exceptions import (
    AuthFailure,
    BadGatewayError,
    ExpiredTokenError,
    InternalServerError,
    InvalidRequest,
    InvalidTokenError,
    InvalidXmlResponse,
    NoPrivilegeError,
    NotFoundItemError,
    OfflineServiceError,
    PleaseTryAgainLaterError,
    RateLimitError,
    SageIntacctSDKError,
    WrongParamsError,
)

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

PAGE_SIZE = 1000


class IntacctOffsetPaginator(BaseOffsetPaginator):  # noqa: D101
    def __init__(  # noqa: ANN204, D107
        self,
        *args: t.Any,
        logger_name: str | None = None,
        **kwargs: t.Any,
    ):
        self.logger = logging.getLogger(logger_name or __name__)
        super().__init__(*args, **kwargs)

    def has_more(self, response: requests.Response) -> bool:
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        total = parsed_response["response"]["operation"]["result"]["data"].get("@totalcount", 0)
        remaining = parsed_response["response"]["operation"]["result"]["data"].get(
            "@numremaining", 0
        )
        progress = int(total) - int(remaining)
        self.logger.info("%d of total %s records processed", progress, total)
        return int(remaining) > 0


class IntacctStream(RESTStream):
    """Intacct stream class."""

    # Update this value if necessary or override `parse_response`.
    rest_method = "POST"
    path = None

    def __init__(
        self,
        *args: t.Any,
        intacct_obj_name: str | None = None,
        replication_key: str | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize stream."""
        super().__init__(*args, **kwargs)
        self.primary_keys = KEY_PROPERTIES[self.name]
        self.intacct_obj_name = intacct_obj_name
        self.replication_key = replication_key
        self.session_id = self._get_session_id()
        self.datetime_fields = [
            i for i, t in self.schema["properties"].items() if t.get("format", "") == "date-time"
        ]
        self.numeric_fields = [
            i
            for i, t in self.schema["properties"].items()
            if "number" in t.get("type", "")
        ]

    @property
    def is_sorted(self) -> bool:
        """Expect stream to be sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
        """
        return True

    def _get_session_id(self) -> str:
        timestamp = datetime.now(timezone.utc)
        dict_body = {
            "request": {
                "control": {
                    "senderid": self.config["sender_id"],
                    "password": self.config["sender_password"],
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {
                        "login": {
                            "userid": self.config["user_id"],
                            "companyid": self.config["company_id"],
                            "password": self.config["user_password"],
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
        payload_data = xmltodict.unparse(dict_body)
        response = requests.post(
            self.url_base,
            headers=self.http_headers,
            data=payload_data,
            timeout=30,
        )
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        if (
            parsed_response["response"]["control"]["status"] == "success"
            and parsed_response["response"]["operation"]["authentication"]["status"] == "success"
        ):
            return parsed_response["response"]["operation"]["result"]["data"]["api"]["sessionid"]

        msg = f"Error: {parsed_response['errormessage']}"
        raise SageIntacctSDKError(msg)

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {"content-type": "application/xml"}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return IntacctOffsetPaginator(
            start_value=0,
            page_size=PAGE_SIZE,
            logger_name=f"{self.tap_name}.{self.name}",
        )

    def _format_date_for_intacct(self, datetime: datetime) -> str:
        """Intacct expects datetimes in a 'MM/DD/YY HH:MM:SS' string format.

        Args:
            datetime: The datetime to be converted.

        Returns:
            'MM/DD/YY HH:MM:SS' formatted string.
        """
        return datetime.strftime("%m/%d/%Y %H:%M:%S")

    def prepare_request(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> requests.PreparedRequest:
        """Prepare a request object for this stream.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict | str = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            # Note: Had to override this method to switch this to data instead of json
            data=request_data,
        )

    def _get_query_filter(
        self,
        rep_key: str,
        context: Context | None,
    ) -> dict:
        return {
            "greaterthanorequalto": {
                "field": rep_key,
                "value": self._format_date_for_intacct(self.get_starting_timestamp(context)),
            }
        }

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        if self.name == "audit_history":
            raise Exception("TODO hanlde audit streams")  # noqa: EM101, TRY002, TRY003

        rep_key = REP_KEYS.get(self.name, GET_BY_DATE_FIELD)
        orderby = {
            "order": {
                "field": rep_key,
                "ascending": {},
            }
        }
        query_filter = self._get_query_filter(rep_key, context)
        data = {
            "query": {
                "object": self.intacct_obj_name,
                "select": {"field": list(self.schema["properties"])},
                "options": {"showprivate": "true"},
                "filter": query_filter,
                "pagesize": PAGE_SIZE,
                "offset": next_page_token,
                "orderby": orderby,
            }
        }
        key = next(iter(data))
        timestamp = datetime.now(timezone.utc)
        dict_body = {
            "request": {
                "control": {
                    "senderid": self.config["sender_id"],
                    "password": self.config["sender_password"],
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"sessionid": self.session_id},
                    "content": {"function": {"@controlid": str(uuid.uuid4()), key: data[key]}},
                },
            }
        }
        return xmltodict.unparse(dict_body)

    @staticmethod
    def support_id_msg(errormessages: dict) -> dict:
        """Get the support ID message from the error message."""
        error = {}
        if isinstance(errormessages["error"], list):
            error["error"] = errormessages["error"][0]
            error["type"] = "list"
        elif isinstance(errormessages["error"], dict):
            error["error"] = errormessages["error"]
            error["type"] = "dict"

        return error

    @staticmethod
    def decode_support_id(errormessages: dict) -> list | dict:
        """Decode the support ID from the error message."""
        support_id_msg = IntacctStream.support_id_msg(errormessages)
        data_type = support_id_msg["type"]
        error = support_id_msg["error"]
        if error and error["description2"]:
            message = error["description2"]
            support_id = re.search("Support ID: (.*)]", message)
            if support_id and support_id.group(1):
                decoded_support_id = unquote(support_id.group(1))
                message = message.replace(support_id.group(1), decoded_support_id)

        if data_type == "list":
            errormessages["error"][0]["description2"] = message if message else None
        elif data_type == "dict":
            errormessages["error"]["description2"] = message if message else None

        return errormessages

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:  # noqa: C901, PLR0912, PLR0915
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        try:
            parsed_xml = xmltodict.parse(response.text)
            parsed_response = json.loads(json.dumps(parsed_xml))
        except Exception as e:
            if response.status_code == http.HTTPStatus.BAD_GATEWAY:
                msg = f"Response status code: {response.status_code}, response: {response.text}"
                raise BadGatewayError(msg) from e

            if response.status_code == http.HTTPStatus.SERVICE_UNAVAILABLE:
                msg = f"Response status code: {response.status_code}, response: {response.text}"
                raise OfflineServiceError(msg) from e

            if response.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                msg = f"Response status code: {response.status_code}, response: {response.text}"
                raise RateLimitError(msg) from e

            msg = f"Response status code: {response.status_code}, response: {response.text}"
            raise InvalidXmlResponse(msg) from e

        if response.status_code == http.HTTPStatus.OK:
            if parsed_response["response"]["control"]["status"] == "success":
                api_response = parsed_response["response"]["operation"]

            if parsed_response["response"]["control"]["status"] == "failure":
                exception_msg = self.decode_support_id(parsed_response["response"]["errormessage"])
                raise WrongParamsError(  # noqa: TRY003
                    "Some of the parameters are wrong",  # noqa: EM101
                    exception_msg,
                )

            if api_response["authentication"]["status"] == "failure":
                raise InvalidTokenError(  # noqa: TRY003
                    "Invalid token / Incorrect credentials",  # noqa: EM101
                    api_response["errormessage"],
                )

            if api_response["result"]["status"] == "success":
                return api_response["result"]["data"].get(self.intacct_obj_name, [])

            self.logger.error("Intacct error response: %s", api_response)
            error = api_response.get("result", {}).get("errormessage", {}).get("error", {})
            desc_2 = (  # noqa: F841
                error.get("description2")
                if isinstance(error, dict)
                else error[0].get("description2")
                if isinstance(error, list) and error
                else ""
            )
            # if (
            #     api_response['result']['status'] == 'failure'
            #     and error
            #     and "There was an error processing the request"
            #     in desc_2
            #     and dict_body["request"]["operation"]["content"]["function"]["query"][
            #         "object"
            #     ]
            #     == "AUDITHISTORY"
            # ):
            #     return {"result": "skip_and_paginate"}

        exception_msg = parsed_response.get("response", {}).get("errormessage", {}).get("error", {})
        correction = exception_msg.get("correction", {})  # type: ignore[union-attr]

        if response.status_code == http.HTTPStatus.BAD_REQUEST:
            if exception_msg.get("errorno") == "GW-0011":  # type: ignore[union-attr]
                msg = (
                    "One or more authentication values are incorrect. "
                    f"Response:{parsed_response}"
                )
                raise AuthFailure(msg)
            raise InvalidRequest("Invalid request", parsed_response)  # noqa: EM101, TRY003

        if response.status_code == http.HTTPStatus.UNAUTHORIZED:
            msg = f"Invalid token / Incorrect credentials. Response: {parsed_response}"
            raise InvalidTokenError(msg)

        if response.status_code == http.HTTPStatus.FORBIDDEN:
            msg = f"Forbidden, the user has insufficient privilege. Response: {parsed_response}"
            raise NoPrivilegeError(msg)

        if response.status_code == http.HTTPStatus.NOT_FOUND:
            msg = f"Not found item with ID. Response: {parsed_response}"
            raise NotFoundItemError(msg)

        if response.status_code == 498:  # noqa: PLR2004
            msg = f"Expired token, try to refresh it. Response: {parsed_response}"
            raise ExpiredTokenError(msg)

        if response.status_code == http.HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = f"Internal server error. Response: {parsed_response}"
            raise InternalServerError(msg)

        if correction and "Please Try Again Later" in correction:
            raise PleaseTryAgainLaterError(parsed_response)

        msg = f"Error: {parsed_response}"
        raise SageIntacctSDKError(msg)

    def _parse_to_datetime(self, date_str: str) -> datetime:
        # Try to parse with the full format first
        try:
            return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        # .replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            # If it fails, try the date-only format
            try:
                return datetime.strptime(date_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
            # .replace(tzinfo=datetime.timezone.utc)
            except ValueError as err:
                # Handle cases where the format is still incorrect
                msg = f"Invalid date format: {date_str}"
                raise ValueError(msg) from err

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        for field in self.datetime_fields:
            if row[field] is not None:
                row[field] = self._parse_to_datetime(row[field])
        for field in self.numeric_fields:
            if row[field] is not None:
                row[field] = float(row[field])
        return row


class GeneralLedgerDetailsStream(IntacctStream):
    """General Ledger Details stream."""

    def __init__(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Initialize stream."""
        # Add MODULEKEY to discovered schema so it can be manually added in post_process
        kwargs["schema"]["properties"]["MODULEKEY"] = th.StringType().to_dict()
        super().__init__(*args, **kwargs)

    def _get_query_filter(
        self,
        rep_key: str,
        context: Context | None,
    ) -> dict:
        return {
            "and": {
                **super()._get_query_filter(rep_key, context),
                "equalto": {
                    "field": "MODULEKEY",
                    "value": context["MODULEKEY"],  # type: ignore[index]
                },
            }
        }

    @property
    def partitions(self) -> list[dict] | None:
        """Get stream partitions.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.

        Returns:
            A list of partition key dicts (if applicable), otherwise `None`.
        """
        return [
            {"MODULEKEY": "2.GL", "name": "General Ledger"},
            {"MODULEKEY": "3.AP", "name": "Accounts Payable"},
            {"MODULEKEY": "4.AR", "name": "Accounts Receivable"},
            {"MODULEKEY": "6.EE", "name": "Employee Expenses"},
            {"MODULEKEY": "7.INV", "name": "Inventory Control"},
            {"MODULEKEY": "8.SO", "name": "Order Entry"},
            {"MODULEKEY": "9.PO", "name": "Purchasing"},
            {"MODULEKEY": "11.CM", "name": "Cash Management"},
            {"MODULEKEY": "48.PROJACCT", "name": "Project and Resource Management"},
            {"MODULEKEY": "55.CONTRACT", "name": "Contracts and Revenue Management"},
        ]
