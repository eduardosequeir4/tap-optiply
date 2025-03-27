"""Test fixtures."""

import pytest
from datetime import datetime, timezone

from tap_optiply.tap import TapOptiply

@pytest.fixture
def tap_class():
    """Return the tap class."""
    return TapOptiply

@pytest.fixture
def sample_config():
    """Return a sample config."""
    return {
        "start_date": datetime.now(timezone.utc).isoformat(),
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "account_id": "test_account_id",
        "username": "test_username",
        "access_token": "test_access_token",
    } 