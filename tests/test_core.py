"""Test core tap functionality using the SDK's built-in test suite."""

from datetime import datetime, timezone

from singer_sdk.testing import get_tap_test_class

from tap_optiply.tap import TapOptiply

SAMPLE_CONFIG = {
    "start_date": datetime.now(timezone.utc).isoformat(),
    "client_id": "test_client_id",
    "client_secret": "test_client_secret",
    "account_id": "test_account_id",
    "username": "test_username",
    "access_token": "test_access_token",
}

TestTapOptiply = get_tap_test_class(
    tap_class=TapOptiply,
    config=SAMPLE_CONFIG,
)

# Run standard built-in tap tests from the SDK:
TestTapOptiply.test_cli_print_version()
TestTapOptiply.test_cli_print_about()
TestTapOptiply.test_cli_discovery()
TestTapOptiply.test_cli_streams()
TestTapOptiply.test_cli_test_connection()

# TODO: Create additional tests as appropriate for your tap.
