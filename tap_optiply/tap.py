"""Optiply tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_optiply import streams


class TapOptiply(Tap):
    """Optiply tap class."""

    name = "tap-optiply"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="The client ID for the OAuth application",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="The client secret for the OAuth application",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The username for the OAuth application",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="The password for the OAuth application",
        ),
        th.Property(
            "access_token",
            th.StringType,
            description="The access token for the OAuth application",
        ),
        th.Property(
            "token_expires_at",
            th.IntegerType,
            description="The timestamp when the access token expires",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_updates = {}
        self.config_file = kwargs.get('config_file')

    def discover_streams(self) -> list[streams.OptiplyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ProductsStream(self),
            streams.SuppliersStream(self),
            streams.SupplierProductsStream(self),
            streams.SellOrdersStream(self),
            streams.SellOrderLinesStream(self),
            streams.BuyOrdersStream(self),
            streams.BuyOrderLinesStream(self),
            streams.ReceiptLinesStream(self),
            streams.ProductCompositionsStream(self),
            streams.PromotionsStream(self),
            streams.PromotionProductsStream(self),
        ]

    def update_token_state(self, access_token: str, token_expires_at: int) -> None:
        """Update the token state.

        Args:
            access_token: The new access token
            token_expires_at: The timestamp when the token expires
        """
        self.state["access_token"] = access_token
        self.state["token_expires_at"] = token_expires_at
        self.write_message({
            "type": "STATE",
            "value": self.state
        })


if __name__ == "__main__":
    TapOptiply.cli()
