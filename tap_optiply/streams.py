"""Stream type classes for tap-tapoptiply."""

from __future__ import annotations

import typing as t
from importlib import resources
from dateutil.parser import parse
from typing import Iterator

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream

from tap_optiply.client import OptiplyAPI, OptiplyStream

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

    def get_starting_time(self, context: t.Optional[dict] = None) -> t.Optional[str]:
        """Get the starting time for incremental sync.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            The starting time in ISO format.
        """
        config = dict(self.config)
        start_date = config.get("start_date")
        if start_date:
            start_date = parse(start_date)
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def _prepare_record(self, record: dict) -> dict:
        """Prepare a record for output by copying updatedAt from attributes if needed.

        Args:
            record: The record to prepare.

        Returns:
            The prepared record.
        """
        # Copy updatedAt from attributes to root level if it exists
        if "attributes" in record and "updatedAt" in record["attributes"]:
            record["updatedAt"] = record["attributes"]["updatedAt"]
        return record

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Records.
        """
        params = {}
        start_date = self.get_starting_time(context)
        if start_date:
            # Ensure replication_key is defined and use it in the filter
            if hasattr(self, 'replication_key') and self.replication_key:
                params[f"filter[{self.replication_key}][GT]"] = start_date.isoformat()
            else:
                # Fallback to updatedAt if replication_key is not defined
                params["filter[updatedAt][GT]"] = start_date.isoformat()
        
        # Always include account_id from the API client if available
        if hasattr(self.api, '_account_id') and self.api._account_id:
            params["filter[accountId]"] = self.api._account_id
        # Fallback to context if available
        elif context and context.get("account_id"):
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_records(self.name, params):
            yield self._prepare_record(record)


class OrdersStream(OptiplyStream):
    """Orders stream."""

    name = "orders"
    path = "/orders"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "orderNumber": {"type": "string"},
            "status": {"type": "string"},
            "createdAt": {"type": "string", "format": "date-time"},
            "updatedAt": {"type": "string", "format": "date-time"},
        },
        "required": ["id", "orderNumber", "status", "createdAt", "updatedAt"],
    }

    def get_url_params(
        self, context: t.Optional[t.Dict[str, t.Any]] = None
    ) -> t.Dict[str, t.Any]:
        """Get URL parameters for the request."""
        params = super().get_url_params(context)
        params["limit"] = 100
        return params


class ProductsStream(OptiplyStream):
    """Products stream."""

    name = "products"
    path = "/products"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "sku": {"type": "string"},
            "name": {"type": "string"},
            "createdAt": {"type": "string", "format": "date-time"},
            "updatedAt": {"type": "string", "format": "date-time"},
        },
        "required": ["id", "sku", "name", "createdAt", "updatedAt"],
    }

    def __init__(self, tap: t.Any, api: OptiplyAPI, **kwargs: t.Any) -> None:
        """Initialize the stream.

        Args:
            tap: The tap instance.
            api: The API client instance.
            **kwargs: Stream keyword arguments.
        """
        super().__init__(tap=tap, api=api, **kwargs)

    def get_url_params(
        self, context: t.Optional[t.Dict[str, t.Any]] = None
    ) -> t.Dict[str, t.Any]:
        """Get URL parameters for the request."""
        params = super().get_url_params(context)
        params["limit"] = 100
        return params

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
        start_date = self.get_starting_timestamp(context)
        if start_date:
            params["filter[updatedAt][GT]"] = start_date.isoformat()

        for record in self.api.get_records(self.name, params):
            yield self._prepare_record(record)


class SuppliersStream(TapOptiplyStream):
    """Define suppliers stream."""

    name = "suppliers"
    path = "suppliers"
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
        yield from super().get_records(context)


class SupplierProductsStream(TapOptiplyStream):
    """Define supplier products stream."""

    name = "supplier_products"
    path = "supplierProducts"
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
            th.Property("weight", th.StringType, description="Product weight"),
            th.Property("volume", th.StringType, description="Product volume"),
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
        yield from super().get_records(context)


class BuyOrdersStream(TapOptiplyStream):
    """Define buy orders stream."""

    name = "buy_orders"
    path = "buyOrders"
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
            th.Property("expectedDeliveryDate", th.DateTimeType, description="Expected delivery date of the order"),
            th.Property("supplierId", th.IntegerType, description="The ID of the supplier"),
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
        yield from super().get_records(context)


class SellOrdersStream(TapOptiplyStream):
    """Define sell orders stream."""

    name = "sell_orders"
    path = "sellOrders"
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
            th.Property("assembly", th.BooleanType, description="Whether this is an assembly order"),
            th.Property("updatedAt", th.DateTimeType, description="When the sell order was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("customer", th.ObjectType(
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
            th.Property("sellOrderLines", th.ObjectType(
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
        yield from super().get_records(context)


class BuyOrderLinesStream(TapOptiplyStream):
    """Define buy order lines stream."""

    name = "buy_order_lines"
    path = "buyOrderLines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The buy order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the buy order line was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("createdAt", th.DateTimeType, description="When the line was created"),
            th.Property("uuid", th.StringType, description="The buy order line's UUID"),
            th.Property("quantity", th.NumberType, description="The quantity ordered"),
            th.Property("productId", th.IntegerType, description="The ID of the product"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("subtotalValue", th.StringType, description="The subtotal value of the line"),
            th.Property("buyOrderId", th.IntegerType, description="The ID of the buy order"),
            th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("product", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("receiptLines", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("buyOrder", th.ObjectType(
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

    def get_records(self, context: t.Optional[dict] = None) -> t.Iterable[dict]:
        """Yield buy order line records."""
        yield from super().get_records(context)


class SellOrderLinesStream(TapOptiplyStream):
    """Define sell order lines stream."""

    name = "sell_order_lines"
    path = "sellOrderLines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The sell order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the sell order line was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("createdAt", th.DateTimeType, description="When the line was created"),
            th.Property("uuid", th.StringType, description="The sell order line's UUID"),
            th.Property("quantity", th.NumberType, description="The quantity ordered"),
            th.Property("productId", th.IntegerType, description="The ID of the product"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("subtotalValue", th.StringType, description="The subtotal value of the line"),
            th.Property("sellOrderId", th.IntegerType, description="The ID of the sell order"),
            th.Property("updatedAt", th.DateTimeType, description="When the line was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
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

    def get_records(self, context: t.Optional[dict] = None) -> t.Iterable[dict]:
        """Yield sell order line records."""
        yield from super().get_records(context)


class ReceiptLinesStream(TapOptiplyStream):
    """Define receipt lines stream."""

    name = "receipt_lines"
    path = "receiptLines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The receipt line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the receipt line was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("createdAt", th.DateTimeType, description="When the receipt line was created"),
            th.Property("uuid", th.StringType, description="The receipt line's UUID"),
            th.Property("quantity", th.NumberType, description="The quantity received"),
            th.Property("occurred", th.DateTimeType, description="When the receipt occurred"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("buyOrderLineId", th.IntegerType, description="The ID of the buy order line"),
            th.Property("updatedAt", th.DateTimeType, description="When the receipt line was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("buyOrderLine", th.ObjectType(
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
        """Return a generator of receipt line records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Receipt line records.
        """
        yield from super().get_records(context)


class ProductCompositionsStream(TapOptiplyStream):
    """Stream for product compositions."""

    name = "product_compositions"
    path = "productCompositions"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "attributes",
            th.ObjectType(
                th.Property("createdAt", th.DateTimeType),
                th.Property("updatedAt", th.DateTimeType),
                th.Property("compositionId", th.StringType),
                th.Property("quantity", th.NumberType),
            ),
        ),
        th.Property(
            "relationships",
            th.ObjectType(
                th.Property(
                    "composition",
                    th.ObjectType(
                        th.Property(
                            "data",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("type", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        """Return a generator of product composition records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Product composition records.
        """
        yield from super().get_records(context)


class PromotionsStream(TapOptiplyStream):
    """Promotions stream."""

    name = "promotions"
    path = "promotions"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "attributes",
            th.ObjectType(
                th.Property("createdAt", th.DateTimeType),
                th.Property("updatedAt", th.DateTimeType),
                th.Property("name", th.StringType),
                th.Property("description", th.StringType),
                th.Property("startDate", th.DateTimeType),
                th.Property("endDate", th.DateTimeType),
                th.Property("status", th.StringType),
                th.Property("accountId", th.IntegerType),
            ),
        ),
        th.Property(
            "relationships",
            th.ObjectType(
                th.Property(
                    "promotionProducts",
                    th.ObjectType(
                        th.Property(
                            "links",
                            th.ObjectType(
                                th.Property("self", th.StringType),
                                th.Property("related", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "account",
                    th.ObjectType(
                        th.Property(
                            "links",
                            th.ObjectType(
                                th.Property("self", th.StringType),
                                th.Property("related", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "links",
            th.ObjectType(
                th.Property("self", th.StringType),
            ),
        ),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterator[dict]:
        """Get records from the API.

        Args:
            context: Stream context.

        Yields:
            Records from the API.
        """
        yield from super().get_records(context)


class PromotionProductsStream(TapOptiplyStream):
    """PromotionProducts stream."""

    name = "promotion_products"
    path = "promotionProducts"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "attributes",
            th.ObjectType(
                th.Property("createdAt", th.DateTimeType),
                th.Property("updatedAt", th.DateTimeType),
                th.Property("promotionId", th.IntegerType),
                th.Property("productId", th.IntegerType),
                th.Property("discountPercentage", th.NumberType),
                th.Property("discountAmount", th.NumberType),
            ),
        ),
        th.Property(
            "relationships",
            th.ObjectType(
                th.Property(
                    "promotion",
                    th.ObjectType(
                        th.Property(
                            "data",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("type", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "product",
                    th.ObjectType(
                        th.Property(
                            "data",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("type", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "links",
            th.ObjectType(
                th.Property("self", th.StringType),
            ),
        ),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterator[dict]:
        """Get records from the API.

        Args:
            context: Stream context.

        Yields:
            Records from the API.
        """
        params = {}
        if context and "promotion_id" in context:
            params["filter[promotionId]"] = context["promotion_id"]
        yield from super().get_records(context)


STREAM_TYPES = {
    "products": ProductsStream,
    "suppliers": SuppliersStream,
    "buy_orders": BuyOrdersStream,
    "buy_order_lines": BuyOrderLinesStream,
    "sell_orders": SellOrdersStream,
    "sell_order_lines": SellOrderLinesStream,
    "receipt_lines": ReceiptLinesStream,
    "supplier_products": SupplierProductsStream,
    "product_compositions": ProductCompositionsStream,
    "promotions": PromotionsStream,
    "promotion_products": PromotionProductsStream,
}