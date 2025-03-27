"""Stream type classes for tap-tapoptiply."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream

from tap_optiply.client import OptiplyAPI

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class TapOptiplyStream(Stream):
    """Stream class for TapOptiply streams."""

    def __init__(self, tap: t.Any, api: OptiplyAPI, context: t.Optional[dict] = None, **kwargs: t.Any) -> None:
        """Initialize the stream.

        Args:
            tap: The tap instance.
            api: The API client instance.
            context: Optional context dictionary.
            **kwargs: Stream keyword arguments.
        """
        super().__init__(tap=tap, **kwargs)
        self.api = api
        self.context = context or {}


class ProductsStream(TapOptiplyStream):
    """Define products stream."""

    name = "products"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The product's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the product was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("name", th.StringType, description="The product's name"),
            th.Property("eanCode", th.StringType, description="The product's EAN code"),
            th.Property("skuCode", th.StringType, description="The product's SKU code"),
            th.Property("articleCode", th.StringType, description="The product's article code"),
            th.Property("price", th.StringType, description="The product's price"),
            th.Property("stockLevel", th.NumberType, description="Current stock level"),
            th.Property("minimumStock", th.IntegerType, description="Minimum stock level"),
            th.Property("maximumStock", th.IntegerType, description="Maximum stock level"),
            th.Property("status", th.StringType, description="The product's status"),
            th.Property("updatedAt", th.DateTimeType, description="When the product was last updated"),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of product records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Product records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_products(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class SuppliersStream(TapOptiplyStream):
    """Define suppliers stream."""

    name = "suppliers"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The supplier's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the supplier was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("name", th.StringType, description="The supplier's name"),
            th.Property("type", th.StringType, description="The supplier's type"),
            th.Property("deliveryTime", th.IntegerType, description="Delivery time in days"),
            th.Property("fixedCosts", th.StringType, description="Fixed costs for orders"),
            th.Property("minimumOrderValue", th.StringType, description="Minimum order value"),
            th.Property("backorders", th.BooleanType, description="Whether backorders are allowed"),
            th.Property("backordersReaction", th.IntegerType, description="Backorders reaction type"),
            th.Property("backorderThreshold", th.IntegerType, description="Backorder threshold"),
            th.Property("createdAt", th.DateTimeType, description="When the supplier was created"),
            th.Property("updatedAt", th.DateTimeType, description="When the supplier was last updated"),
            th.Property("ignored", th.BooleanType, description="Whether the supplier is ignored"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("emails", th.ArrayType(th.StringType), description="Supplier email addresses"),
            th.Property("accountId", th.IntegerType, description="The account ID"),
            th.Property("uuid", th.StringType, description="The supplier's UUID"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("supplierProducts", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("buyOrders", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("account", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of supplier records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Supplier records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_suppliers(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class SupplierProductsStream(TapOptiplyStream):
    """Define supplier products stream."""

    name = "supplier_products"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The supplier product's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the supplier product was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("supplierId", th.IntegerType, description="The ID of the supplier"),
            th.Property("deliveryTime", th.IntegerType, description="Delivery time in days"),
            th.Property("notBeingBought", th.BooleanType, description="Whether the product is not being bought"),
            th.Property("availabilityDate", th.DateTimeType, description="Date when the product becomes available"),
            th.Property("availability", th.BooleanType, description="Whether the product is available"),
            th.Property("freeStock", th.NumberType, description="Amount of free stock"),
            th.Property("uuid", th.StringType, description="The supplier product's UUID"),
            th.Property("createdAt", th.DateTimeType, description="When the supplier product was created"),
            th.Property("eanCode", th.StringType, description="The product's EAN code"),
            th.Property("price", th.StringType, description="The product's price"),
            th.Property("preferred", th.BooleanType, description="Whether this is the preferred supplier product"),
            th.Property("updatedAt", th.DateTimeType, description="When the supplier product was last updated"),
            th.Property("resumingPurchase", th.DateTimeType, description="When to resume purchasing"),
            th.Property("productId", th.IntegerType, description="The ID of the product"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("lotSize", th.IntegerType, description="Lot size for ordering"),
            th.Property("minimumPurchaseQuantity", th.IntegerType, description="Minimum purchase quantity"),
            th.Property("weight", th.NumberType, description="Product weight"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("volume", th.NumberType, description="Product volume"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
            th.Property("name", th.StringType, description="The product's name"),
            th.Property("skuCode", th.StringType, description="The product's SKU code"),
            th.Property("articleCode", th.StringType, description="The product's article code"),
            th.Property("status", th.StringType, description="The product's status"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("product", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("supplier", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of supplier product records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Supplier product records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_supplier_products(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class BuyOrdersStream(TapOptiplyStream):
    """Define buy orders stream."""

    name = "buy_orders"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The buy order's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the buy order was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("totalValue", th.StringType, description="Total value of the order"),
            th.Property("createdAt", th.DateTimeType, description="When the buy order was created"),
            th.Property("accountId", th.IntegerType, description="The account ID"),
            th.Property("placed", th.DateTimeType, description="When the order was placed"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
            th.Property("completed", th.DateTimeType, description="When the order was completed"),
            th.Property("uuid", th.StringType, description="The buy order's UUID"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("assembly", th.BooleanType, description="Whether this is an assembly order"),
            th.Property("updatedAt", th.DateTimeType, description="When the buy order was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("supplier", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("account", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("buyOrderLines", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of buy order records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Buy order records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_buy_orders(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class SellOrdersStream(TapOptiplyStream):
    """Define sell orders stream."""

    name = "sell_orders"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The sell order's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the sell order was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("totalValue", th.StringType, description="Total value of the order"),
            th.Property("createdAt", th.DateTimeType, description="When the sell order was created"),
            th.Property("accountId", th.IntegerType, description="The account ID"),
            th.Property("placed", th.DateTimeType, description="When the order was placed"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
            th.Property("completed", th.DateTimeType, description="When the order was completed"),
            th.Property("uuid", th.StringType, description="The sell order's UUID"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("updatedAt", th.DateTimeType, description="When the sell order was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("sellOrderLines", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("account", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of sell order records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Sell order records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_sell_orders(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class BuyOrderLinesStream(TapOptiplyStream):
    """Define buy order lines stream."""

    name = "buy_order_lines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The buy order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("buyOrderId", th.IntegerType, description="The ID of the buy order"),
            th.Property("productId", th.IntegerType, description="The ID of the product"),
            th.Property("quantity", th.NumberType, description="The quantity ordered"),
            th.Property("price", th.StringType, description="The price per unit"),
            th.Property("totalValue", th.StringType, description="The total value of the line"),
            th.Property("createdAt", th.DateTimeType, description="When the line was created"),
            th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("buyOrder", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("product", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of buy order line records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Buy order line records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_buy_order_lines(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


class SellOrderLinesStream(TapOptiplyStream):
    """Define sell order lines stream."""

    name = "sell_order_lines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The sell order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("sellOrderId", th.IntegerType, description="The ID of the sell order"),
            th.Property("productId", th.IntegerType, description="The ID of the product"),
            th.Property("quantity", th.NumberType, description="The quantity ordered"),
            th.Property("price", th.StringType, description="The price per unit"),
            th.Property("totalValue", th.StringType, description="The total value of the line"),
            th.Property("createdAt", th.DateTimeType, description="When the line was created"),
            th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("sellOrder", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("product", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
        )),
        th.Property("links", th.ObjectType(
            th.Property("self", th.StringType),
        )),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of sell order line records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Sell order line records.
        """
        params = {}
        if context and context.get("start_date"):
            params["filter[updatedAt][GT]"] = context["start_date"]
        if context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_sell_order_lines(params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record
