"""Singer tap for Optiply."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_optiply import streams
from tap_optiply.tap import TapOptiply

TAP_NAME = "tap-optiply"
STREAM_TYPES = ["products", "suppliers", "supplierProducts", "sellOrders", "sellOrderLines", "buyOrders", "buyOrderLines", "receiptLines", "productCompositions", "promotions", "promotionProducts"]

__all__ = ["TapOptiply", "TAP_NAME", "STREAM_TYPES"]
