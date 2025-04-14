"""TapOptiply tap class."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta
import pytz

from singer_sdk import Tap
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
)

from tap_optiply import streams
from tap_optiply.client import OptiplyAPI


class TapOptiply(Tap):
    """Optiply tap class."""

    name = "tap-optiply"

    config_jsonschema = {
        "type": "object",
        "properties": {
            "username": {
                "type": "string",
                "description": "The username for authentication",
            },
            "password": {
                "type": "string",
                "description": "The password for authentication",
                "sensitive": True,
            },
            "account_id": {
                "type": "integer",
                "description": "The account ID to filter requests",
            },
            "client_id": {
                "type": "string",
                "description": "The client ID for authentication",
            },
            "client_secret": {
                "type": "string",
                "description": "The client secret for authentication",
                "sensitive": True,
            },
            "couplingId": {
                "type": "integer",
                "description": "The coupling ID",
            },
            "access_token": {
                "type": "string",
                "description": "The access token for authentication",
                "sensitive": True,
            },
            "refresh_token": {
                "type": "string",
                "description": "The refresh token for authentication",
                "sensitive": True,
            },
            "token_expires_at": {
                "type": "number",
                "description": "Unix timestamp when the access token expires",
            },
            "start_date": {
                "type": "string",
                "description": "The start date for replication key in ISO format (e.g. 2025-02-17T15:31:58Z)",
            },
            "authorization": {
                "type": "string",
                "description": "The authorization token for authentication",
                "sensitive": True,
            }
        },
        "required": ["username", "password", "account_id", "start_date"],
    }

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the tap.

        Args:
            *args: Tap arguments.
            **kwargs: Tap keyword arguments.
        """
        self._api: OptiplyAPI | None = None
        super().__init__(*args, **kwargs)

    @property
    def api(self) -> OptiplyAPI:
        """Get the API client.

        Returns:
            The API client.
        """
        if self._api is None:
            # Get configuration from either nested 'config' object or root level
            config = dict(self.config.get("config", self.config))
            
            # Required parameters
            username = config.get("username")
            password = config.get("password")
            account_id = config.get("account_id")
            
            if not username or not password or not account_id:
                raise ValueError("Missing required configuration: username, password, and account_id are required")
            
            self._api = OptiplyAPI(
                username=username,
                password=password,
                account_id=account_id,
                config=config,
            )
        return self._api

    def discover_streams(self) -> list[streams.TapOptiplyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        # Get configuration from either nested 'config' object or root level
        config = dict(self.config.get("config", self.config))
        
        # Required parameters
        account_id = config.get("account_id")
        if not account_id:
            raise ValueError("account_id is required in configuration")
            
        # Use default start_date if not provided
        start_date = config.get("start_date", "2000-01-01T00:00:00.000Z")
        
        return [
            streams.ProductsStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.SuppliersStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.SupplierProductsStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.BuyOrdersStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.BuyOrderLinesStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.SellOrdersStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.SellOrderLinesStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.ReceiptLinesStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
            streams.ProductCompositionsStream(tap=self, api=self.api, context={"account_id": account_id, "start_date": start_date}),
        ]

    def get_stream_types(self) -> list[type[streams.TapOptiplyStream]]:
        """Return list of available stream types.

        Returns:
            A list of available stream types.
        """
        return [
            streams.ProductsStream,
            streams.SuppliersStream,
            streams.SupplierProductsStream,
            streams.BuyOrdersStream,
            streams.SellOrdersStream,
            streams.BuyOrderLinesStream,
            streams.SellOrderLinesStream,
            streams.ReceiptLinesStream,
            streams.ProductCompositionsStream,
        ]

    def get_stream_metadata(self) -> dict:
        """Get stream metadata.

        Returns:
            Stream metadata.
        """
        config = dict(self.config.get("config", {}))
        if not config:
            raise ValueError("Configuration is missing or empty")
            
        account_id = config.get("account_id")
        if account_id is None:
            raise ValueError("account_id is required in configuration")
            
        start_date = config.get("start_date")
        if start_date is None:
            raise ValueError("start_date is required in configuration")

        return {
            stream.name: {
                "primary_keys": stream.primary_keys,
                "replication_key": stream.replication_key,
                "replication_method": (
                    "INCREMENTAL"
                    if stream.replication_key
                    else "FULL_TABLE"
                ),
                "is_view": False,
                "schema": stream.schema,
                "stream_type": "object",
                "tap_stream_id": stream.name,
                "database_name": "optiply",
                "table_name": stream.name,
                "metadata": {
                    "table-key-properties": stream.primary_keys,
                    "forced-replication-method": (
                        "INCREMENTAL"
                        if stream.replication_key
                        else "FULL_TABLE"
                    ),
                    "valid-replication-keys": [stream.replication_key]
                    if stream.replication_key
                    else None,
                    "schema-name": stream.name,
                },
                "key_properties": stream.primary_keys,
            }
            for stream in self.streams
        }

    def get_capabilities(self) -> list[CapabilitiesEnum]:
        """Get tap capabilities.

        Returns:
            A list of capabilities.
        """
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.STATE,
            TapCapabilities.DISCOVER,
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
        ]


if __name__ == "__main__":
    TapOptiply.cli()
