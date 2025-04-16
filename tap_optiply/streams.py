"""Stream type classes for tap-optiply."""

from __future__ import annotations

import typing as t
from datetime import datetime
from importlib import resources

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_optiply.client import OptiplyStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ProductsStream(OptiplyStream):
    """Define products stream."""

    name = "products"
    path = "/products"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.data[*]"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("ignored", th.BooleanType),
        th.Property("uuid", th.StringType),
        th.Property("notBeingBought", th.BooleanType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("stockLevel", th.NumberType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("accountId", th.IntegerType),
        th.Property("eanCode", th.StringType),
        th.Property("price", th.StringType),
        th.Property("name", th.StringType),
        th.Property("minimumStock", th.NumberType),
        th.Property("assembled", th.BooleanType),
        th.Property("stockMeasurementUnit", th.StringType),
        th.Property("category", th.StringType),
        th.Property("skuCode", th.StringType),
        th.Property("articleCode", th.StringType),
        th.Property("novel", th.BooleanType),
        th.Property("unlimitedStock", th.BooleanType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("resumingPurchase", th.StringType),
        th.Property("status", th.StringType),
        th.Property("createdAtRemote", th.DateTimeType),
        th.Property("manualServiceLevel", th.NumberType),
        th.Property("remoteIdMap", th.ObjectType()),
        th.Property("remoteDataSyncedToDate", th.DateTimeType),
        th.Property("maximumStock", th.NumberType),
    ).to_dict()


class SuppliersStream(OptiplyStream):
    """Define custom stream."""

    name = "suppliers"
    path = "/suppliers"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.data[*]"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("maxLoadCapacity", th.NumberType),
        th.Property("ignored", th.BooleanType),
        th.Property("uuid", th.StringType),
        th.Property("deliveryTime", th.NumberType),
        th.Property("type", th.StringType),
        th.Property("globalLocationNumber", th.StringType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("lostSalesReaction", th.AnyOf(th.StringType, th.NumberType)),
        th.Property("fixedCosts", th.NumberType),
        th.Property("userReplenishmentPeriod", th.NumberType),
        th.Property("lostSalesMovReaction", th.StringType),
        th.Property("emails", th.ArrayType(th.StringType)),
        th.Property("minimumOrderValue", th.StringType),
        th.Property("containerVolume", th.NumberType),
        th.Property("accountId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("backorders", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property("reactingToLostSales", th.BooleanType),
        th.Property("backordersReaction", th.StringType),
        th.Property("backorderThreshold", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("remoteIdMap", th.ObjectType()),
        th.Property("remoteDataSyncedToDate", th.DateTimeType),
    ).to_dict()


class SupplierProductsStream(OptiplyStream):
    """Define supplier products stream."""

    name = "supplierProducts"
    path = "/supplierProducts"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.data[*]"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uuid", th.StringType),
        th.Property("supplierId", th.IntegerType),
        th.Property("deliveryTime", th.NumberType),
        th.Property("notBeingBought", th.BooleanType),
        th.Property("availabilityDate", th.DateTimeType),
        th.Property("availability", th.BooleanType),
        th.Property("freeStock", th.NumberType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("eanCode", th.StringType),
        th.Property("price", th.StringType),
        th.Property("preferred", th.BooleanType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("resumingPurchase", th.StringType),
        th.Property("productId", th.IntegerType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("lotSize", th.NumberType),
        th.Property("minimumPurchaseQuantity", th.NumberType),
        th.Property("weight", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("skuCode", th.StringType),
        th.Property("articleCode", th.StringType),
        th.Property("status", th.StringType),
        th.Property("remoteIdMap", th.ObjectType()),
        th.Property("remoteDataSyncedToDate", th.DateTimeType),
    ).to_dict()


class SellOrdersStream(OptiplyStream):
    """Define sell orders stream."""

    name = "sellOrders"
    path = "/sellOrders"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.data[*]"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("totalValue", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("accountId", th.IntegerType),
        th.Property("uuid", th.StringType),
        th.Property("placed", th.DateTimeType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("completed", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("remoteDataSyncedToDate", th.DateTimeType),
        th.Property("remoteIdMap", th.ObjectType()),
    ).to_dict()


class BuyOrdersStream(OptiplyStream):
    """Buy Orders stream."""

    name = "buyOrders"
    path = "/buyOrders"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("totalValue", th.StringType),
        th.Property("accountId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("placed", th.DateTimeType),
        th.Property("supplierId", th.IntegerType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("assembly", th.BooleanType),
        th.Property("expectedDeliveryDate", th.DateTimeType),
        th.Property("completed", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class BuyOrderLinesStream(OptiplyStream):
    """Buy Order Lines stream."""

    name = "buyOrderLines"
    path = "/buyOrderLines"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("productId", th.IntegerType),
        th.Property("buyOrderId", th.IntegerType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("subtotalValue", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class ReceiptLinesStream(OptiplyStream):
    """Receipt Lines stream."""

    name = "receiptLines"
    path = "/receiptLines"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("occurred", th.DateTimeType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("buyOrderLineId", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class ProductCompositionsStream(OptiplyStream):
    """Product Compositions stream."""

    name = "productCompositions"
    path = "/productCompositions"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("composedProductId", th.IntegerType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("partProductId", th.IntegerType),
        th.Property("partQuantity", th.NumberType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class PromotionsStream(OptiplyStream):
    """Promotions stream."""

    name = "promotions"
    path = "/promotions"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uuid", th.StringType),
        th.Property("endDate", th.DateTimeType),
        th.Property("upliftType", th.StringType),
        th.Property("productCount", th.IntegerType),
        th.Property("enabled", th.BooleanType),
        th.Property("accountId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("upliftIncrease", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("startDate", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class PromotionProductsStream(OptiplyStream):
    """Promotion Products stream."""

    name = "promotionProducts"
    path = "/promotionProducts"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("specificUpliftType", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("productId", th.IntegerType),
        th.Property("specificUpliftIncrease", th.NumberType),
        th.Property("promotionId", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class SellOrderLinesStream(OptiplyStream):
    """Define sell order lines stream."""

    name = "sellOrderLines"
    path = "/sellOrderLines"
    primary_keys = ["id"]
    replication_key = "updatedAt"
    records_jsonpath = "$.data[*]"
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("uuid", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("productId", th.IntegerType),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("subtotalValue", th.StringType),
        th.Property("sellOrderId", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()
