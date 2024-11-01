"""Intacct tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_intacct import streams


class TapIntacct(Tap):
    """Intacct tap class."""

    name = "tap-intacct"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
            title="Project IDs",
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.IntacctStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapIntacct.cli()
