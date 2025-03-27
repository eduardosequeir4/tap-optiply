"""Test stream functionality."""

from datetime import datetime, timezone

import pytest
from singer_sdk.testing import get_standard_tap_tests

from tap_optiply.tap import TapOptiply

SAMPLE_CONFIG = {
    "start_date": datetime.now(timezone.utc).isoformat(),
    "client_id": "test_client_id",
    "client_secret": "test_client_secret",
    "account_id": "test_account_id",
    "username": "test_username",
    "access_token": "test_access_token",
}

# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests using the SDK."""
    tests = get_standard_tap_tests(TapOptiply, config=SAMPLE_CONFIG)
    for test in tests:
        test()

@pytest.mark.parametrize(
    "stream_name,expected_records",
    [
        ("products", 0),
        ("suppliers", 0),
        ("buyOrders", 0),
        ("sellOrders", 0),
        ("buyOrderLines", 0),
        ("sellOrderLines", 0),
    ],
)
def test_streams(tap_class, stream_name, expected_records):
    """Test that streams return the expected number of records."""
    tap = tap_class(config=SAMPLE_CONFIG)
    stream = tap.get_stream(stream_name)
    records = list(stream.get_records(context={"start_date": SAMPLE_CONFIG["start_date"]}))
    assert len(records) >= expected_records 