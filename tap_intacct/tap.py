"""Intacct tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_intacct import streams
from tap_intacct.const import INTACCT_OBJECTS
from tap_intacct.sage import get_client


class TapIntacct(Tap):
    """Intacct tap class."""

    name = "tap-intacct"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.intacct.com/ia/xml/xmlgw.phtml",
            description="The Intacct API URL",
        ),
        th.Property(
            "company_id",
            th.StringType,
            description="The Intacct company ID",
        ),
        th.Property(
            "sender_id",
            th.StringType,
            description="The Intacct sender ID",
        ),
        th.Property(
            "sender_password",
            th.StringType,
            secret=True,
            description="The Intacct sender password",
        ),
        th.Property(
            "user_id",
            th.StringType,
            description="The Intacct user ID",
        ),
        th.Property(
            "user_password",
            th.StringType,
            secret=True,
            description="The Intacct user password",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        discovered_streams = []
        sage_client = get_client(
            api_url=self.config["api_url"],
            company_id=self.config["company_id"],
            sender_id=self.config["sender_id"],
            sender_password=self.config["sender_password"],
            user_id=self.config["user_id"],
            user_password=self.config["user_password"],
            headers={},
        )
        for stream_name in INTACCT_OBJECTS:
            schema = sage_client.load_schema_from_api(stream_name)
            stream = streams.IntacctStream(
                tap=self,
                name=stream_name,
                schema=schema,
                intacct_obj_name=INTACCT_OBJECTS[stream_name],
                replication_key="WHENMODIFIED",
            )
            discovered_streams.append(stream)
            # audit_stream = streams.SageStream(
            #     tap=self,
            #     name=f"audit_history_{stream_name}",
            #     schema=schema,
            #     intacct_obj_name="AUDITHISTORY",
            #     replication_key="ACCESSTIME",
            # )
            # discovered_streams.append(audit_stream)
        return discovered_streams


if __name__ == "__main__":
    TapIntacct.cli()
