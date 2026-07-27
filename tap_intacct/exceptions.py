"""Sage Intacct SDK Exceptions."""


class SageIntacctSDKError(Exception):
    """The base exception class for SageIntacctSDK.

    Parameters:
        msg (str): Short description of the error.
        response: Error response from the API call.
    """

    def __init__(self, msg, response=None):  # ruff: ignore[missing-type-function-argument, missing-return-type-special-method, undocumented-public-init]
        super(SageIntacctSDKError, self).__init__(msg)  # ruff: ignore[super-call-with-parameters]
        self.message = msg
        self.response = response

    def __str__(self):  # ruff: ignore[missing-return-type-special-method, undocumented-magic-method]
        return repr(self.message)


class ExpiredTokenError(SageIntacctSDKError):
    """Expired (old) access token, 498 error."""


class InvalidTokenError(SageIntacctSDKError):
    """Wrong/non-existing access token, 401 error."""


class NoPrivilegeError(SageIntacctSDKError):
    """The user has insufficient privilege, 403 error."""


class WrongParamsError(SageIntacctSDKError):
    """Some of the parameters (HTTP params or request body) are wrong, 400 error."""


class NotFoundItemError(SageIntacctSDKError):
    """Not found the item from URL, 404 error."""


class InternalServerError(SageIntacctSDKError):
    """The rest SageIntacctSDK errors, 500 error."""


class InvalidRequest(SageIntacctSDKError):  # ruff: ignore[error-suffix-on-exception-name]
    """The rest SageIntacctSDK errors, 500 error."""


class AuthFailure(SageIntacctSDKError):  # ruff: ignore[error-suffix-on-exception-name]
    """The rest SageIntacctSDK errors, 500 error."""


class InvalidXmlResponse(Exception):  # ruff: ignore[error-suffix-on-exception-name, undocumented-public-class]
    pass


class BadGatewayError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


class OfflineServiceError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


class RateLimitError(Exception):  # ruff: ignore[undocumented-public-class]
    pass


class PleaseTryAgainLaterError(Exception):  # ruff: ignore[undocumented-public-class]
    pass
