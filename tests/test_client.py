"""Test client functionality."""

import pytest
from singer_sdk.testing import get_standard_tap_tests

from tap_optiply.client import TapOptiplyClient
from tap_optiply.tap import TapOptiply

SAMPLE_CONFIG = {
    "start_date": "2025-03-01T00:00:00Z",
    "client_id": "test_client_id",
    "client_secret": "test_client_secret",
    "account_id": "test_account_id",
    "username": "test_username",
    "access_token": "test_access_token",
}

def test_client_initialization():
    """Test that the client can be initialized."""
    client = TapOptiplyClient(
        client_id=SAMPLE_CONFIG["client_id"],
        client_secret=SAMPLE_CONFIG["client_secret"],
        account_id=SAMPLE_CONFIG["account_id"],
        username=SAMPLE_CONFIG["username"],
        access_token=SAMPLE_CONFIG["access_token"],
    )
    assert client is not None
    assert client.client_id == SAMPLE_CONFIG["client_id"]
    assert client.client_secret == SAMPLE_CONFIG["client_secret"]
    assert client.account_id == SAMPLE_CONFIG["account_id"]
    assert client.username == SAMPLE_CONFIG["username"]
    assert client.access_token == SAMPLE_CONFIG["access_token"]

@pytest.mark.parametrize(
    "stream_name",
    [
        "products",
        "suppliers",
        "buyOrders",
        "sellOrders",
        "buyOrderLines",
        "sellOrderLines",
    ],
)
def test_client_streams(stream_name):
    """Test that the client can get records for each stream."""
    client = TapOptiplyClient(
        client_id=SAMPLE_CONFIG["client_id"],
        client_secret=SAMPLE_CONFIG["client_secret"],
        account_id=SAMPLE_CONFIG["account_id"],
        username=SAMPLE_CONFIG["username"],
        access_token=SAMPLE_CONFIG["access_token"],
    )
    records = list(client.get_records(stream_name, context={"start_date": SAMPLE_CONFIG["start_date"]}))
    assert isinstance(records, list) 