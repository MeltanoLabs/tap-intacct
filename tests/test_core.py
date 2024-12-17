"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_intacct.tap import TapIntacct

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapIntacct = get_tap_test_class(
    tap_class=TapIntacct,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        max_records_limit=15,
    ),
)
