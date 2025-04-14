"""Stream type classes for tap-tapoptiply."""

from __future__ import annotations

import typing as t
from importlib import resources
from dateutil.parser import parse

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
            # Use the correct filter format for date filtering
            params["filter[updatedAt][GT]"] = start_date.isoformat()
        if context and context.get("account_id"):
            # Use the correct filter format for accountId
            params["filter[accountId]"] = context["account_id"]

        for record in self.api.get_records(self.name, params):
            # Copy updatedAt from attributes to root level
            if "attributes" in record and "updatedAt" in record["attributes"]:
                record["updatedAt"] = record["attributes"]["updatedAt"]
            yield record


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
            th.Property("notBeingBought", th.BooleanType, description="Whether the product is not being bought"),
            th.Property("createdAtRemote", th.DateTimeType, description="When the product was created remotely"),
            th.Property("uuid", th.StringType, description="The product's UUID"),
            th.Property("createdAt", th.DateTimeType, description="When the product was created"),
            th.Property("eanCode", th.StringType, description="The product's EAN code"),
            th.Property("price", th.StringType, description="The product's price"),
            th.Property("stockMeasurementUnit", th.StringType, description="The unit of stock measurement"),
            th.Property("minimumStock", th.IntegerType, description="Minimum stock level"),
            th.Property("assembled", th.BooleanType, description="Whether the product is assembled"),
            th.Property("manualServiceLevel", th.StringType, description="Manual service level"),
            th.Property("updatedAt", th.DateTimeType, description="When the product was last updated"),
            th.Property("resumingPurchase", th.DateTimeType, description="When to resume purchasing"),
            th.Property("ignored", th.BooleanType, description="Whether the product is ignored"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("remoteIdMap", th.ObjectType(), description="Remote ID mapping"),
            th.Property("stockLevel", th.NumberType, description="Current stock level"),
            th.Property("accountId", th.IntegerType, description="The account ID"),
            th.Property("remoteDataSyncedToDate", th.DateTimeType, description="When remote data was last synced"),
            th.Property("name", th.StringType, description="The product's name"),
            th.Property("category", th.StringType, description="The product's category"),
            th.Property("maximumStock", th.IntegerType, description="Maximum stock level"),
            th.Property("skuCode", th.StringType, description="The product's SKU code"),
            th.Property("articleCode", th.StringType, description="The product's article code"),
            th.Property("novel", th.BooleanType, description="Whether the product is novel"),
            th.Property("unlimitedStock", th.BooleanType, description="Whether stock is unlimited"),
            th.Property("status", th.StringType, description="The product's status"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("supplierProducts", th.ObjectType(
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
            th.Property("productComposedFromCompositions", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("promotionProducts", th.ObjectType(
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
            th.Property("productPartOfCompositions", th.ObjectType(
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
        """Return a generator of product records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Product records.
        """
        yield from super().get_records(context)


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
        yield from super().get_records(context)


class SupplierProductsStream(TapOptiplyStream):
    """Supplier products stream."""

    name = "supplier_products"
    path = "/supplierProducts"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The supplier product's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the supplier product was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("uuid", th.StringType, description="The supplier product's UUID"),
            th.Property("supplierId", th.IntegerType, description="The supplier ID"),
            th.Property("deliveryTime", th.IntegerType, description="Delivery time in days"),
            th.Property("notBeingBought", th.BooleanType, description="Whether the product is not being bought"),
            th.Property("availabilityDate", th.DateTimeType, description="When the product will be available"),
            th.Property("availability", th.BooleanType, description="Whether the product is available"),
            th.Property("freeStock", th.IntegerType, description="Free stock quantity"),
            th.Property("createdAt", th.DateTimeType, description="When the supplier product was created"),
            th.Property("eanCode", th.StringType, description="The product's EAN code"),
            th.Property("price", th.StringType, description="The product's price"),
            th.Property("preferred", th.BooleanType, description="Whether this is a preferred supplier product"),
            th.Property("updatedAt", th.DateTimeType, description="When the supplier product was last updated"),
            th.Property("resumingPurchase", th.DateTimeType, description="When to resume purchasing"),
            th.Property("productId", th.IntegerType, description="The product ID"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("lotSize", th.IntegerType, description="Lot size for ordering"),
            th.Property("minimumPurchaseQuantity", th.IntegerType, description="Minimum purchase quantity"),
            th.Property("weight", th.NumberType, description="Product weight"),
            th.Property("volume", th.NumberType, description="Product volume"),
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


class ProductCompositionsStream(TapOptiplyStream):
    """Product compositions stream."""

    name = "product_compositions"
    path = "/productCompositions"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The product composition's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the product composition was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("composedProductId", th.IntegerType, description="The composed product ID"),
            th.Property("partProductId", th.IntegerType, description="The part product ID"),
            th.Property("partQuantity", th.IntegerType, description="The quantity of parts needed"),
            th.Property("createdAt", th.DateTimeType, description="When the product composition was created"),
            th.Property("updatedAt", th.DateTimeType, description="When the product composition was last updated"),
            th.Property("createdFromPublicApi", th.BooleanType, description="Whether created from public API"),
            th.Property("uuid", th.StringType, description="The product composition's UUID"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("composedProduct", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("partProduct", th.ObjectType(
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


class PromotionsStream(TapOptiplyStream):
    """Promotions stream."""

    name = "promotions"
    path = "/promotions"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The promotion's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the promotion was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("uuid", th.StringType, description="The promotion's UUID"),
            th.Property("endDate", th.DateTimeType, description="When the promotion ends"),
            th.Property("upliftType", th.StringType, description="The type of uplift (absolute or percentage)"),
            th.Property("productCount", th.IntegerType, description="Number of products in the promotion"),
            th.Property("enabled", th.BooleanType, description="Whether the promotion is enabled"),
            th.Property("accountId", th.IntegerType, description="The account ID"),
            th.Property("createdAt", th.DateTimeType, description="When the promotion was created"),
            th.Property("upliftIncrease", th.StringType, description="The uplift increase amount"),
            th.Property("name", th.StringType, description="The promotion's name"),
            th.Property("startDate", th.DateTimeType, description="When the promotion starts"),
            th.Property("updatedAt", th.DateTimeType, description="When the promotion was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("promotionProducts", th.ObjectType(
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


class PromotionProductStream(TapOptiplyStream):
    """Promotion product stream."""

    name = "promotion_products"
    path = "/promotionProducts"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The promotion product's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
        th.Property("updatedAt", th.DateTimeType, description="When the promotion product was last updated"),
        th.Property("attributes", th.ObjectType(
            th.Property("specificUpliftType", th.StringType, description="The specific uplift type for this product"),
            th.Property("createdAt", th.DateTimeType, description="When the promotion product was created"),
            th.Property("uuid", th.StringType, description="The promotion product's UUID"),
            th.Property("productId", th.IntegerType, description="The product ID"),
            th.Property("specificUpliftIncrease", th.StringType, description="The specific uplift increase for this product"),
            th.Property("promotionId", th.IntegerType, description="The promotion ID"),
            th.Property("updatedAt", th.DateTimeType, description="When the promotion product was last updated"),
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("product", th.ObjectType(
                th.Property("links", th.ObjectType(
                    th.Property("self", th.StringType),
                    th.Property("related", th.StringType),
                )),
            )),
            th.Property("promotion", th.ObjectType(
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

    def get_child_context(self, record: dict, context: t.Optional[dict] = None) -> dict:
        """Return a context dictionary for child streams.

        Args:
            record: The record from the parent stream.
            context: The parent stream's context.

        Returns:
            A context dictionary for child streams.
        """
        return {"buyOrderId": record["id"]}

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

    def get_child_context(self, record: dict, context: t.Optional[dict] = None) -> dict:
        """Return a context dictionary for child streams.

        Args:
            record: The record from the parent stream.
            context: The parent stream's context.

        Returns:
            A context dictionary for child streams.
        """
        return {"sellOrderId": record["id"]}

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
    primary_keys: t.ClassVar[list[str]] = ["id"]
    parent_stream_type = BuyOrdersStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The buy order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
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
        if context and "buyOrderId" in context:
            # Use the correct filter format for buyOrderId
            params = {"filter[buyOrderId]": context["buyOrderId"]}
            for record in self.api.get_records(self.name, params):
                yield record
        else:
            yield from super().get_records(context)


class SellOrderLinesStream(TapOptiplyStream):
    """Define sell order lines stream."""

    name = "sell_order_lines"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    parent_stream_type = SellOrdersStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="The sell order line's unique identifier"),
        th.Property("type", th.StringType, description="The resource type"),
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
            th.Property("sellOrder", th.ObjectType(
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
        if context and "sellOrderId" in context:
            # Use the correct filter format for sellOrderId
            params = {"filter[sellOrderId]": context["sellOrderId"]}
            for record in self.api.get_records(self.name, params):
                yield record
        else:
            yield from super().get_records(context)


class ReceiptLinesStream(TapOptiplyStream):
    """Define receipt lines stream."""

    name = "receipt_lines"
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
