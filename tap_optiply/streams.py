"""Stream type classes for tap-optiply."""

from __future__ import annotations

import typing as t
from datetime import datetime
from importlib import resources
import time

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
        th.Property("manualServiceLevel", th.CustomType({"type": ["string", "integer", "null"]})),
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
        th.Property("maxLoadCapacity", th.CustomType({"type": ["string", "null"]})),
        th.Property("ignored", th.BooleanType),
        th.Property("uuid", th.StringType),
        th.Property("deliveryTime", th.CustomType({"type": ["integer", "null"]})),
        th.Property("type", th.StringType),
        th.Property("globalLocationNumber", th.CustomType({"type": ["string", "null"]})),
        th.Property("createdFromPublicApi", th.BooleanType),
        th.Property("lostSalesReaction", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("fixedCosts", th.CustomType({"type": ["string", "null"]})),
        th.Property("userReplenishmentPeriod", th.CustomType({"type": ["integer", "null"]})),
        th.Property("lostSalesMovReaction", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("emails", th.ArrayType(th.StringType)),
        th.Property("minimumOrderValue", th.StringType),
        th.Property("containerVolume", th.CustomType({"type": ["string", "null"]})),
        th.Property("accountId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("backorders", th.BooleanType),
        th.Property("name", th.CustomType({"type": ["string", "null"]})),
        th.Property("reactingToLostSales", th.BooleanType),
        th.Property("backordersReaction", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("backorderThreshold", th.CustomType({"type": ["string", "integer", "null"]})),
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
        th.Property("weight", th.StringType),
        th.Property("volume", th.StringType),
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

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        next_page_token: int | None = None
        finished = False
        max_updated_at = None
        retry_count = 0

        while not finished:
            try:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=next_page_token,
                )
                self.logger.info(f"Making request to: {prepared_request.url}")
                self.logger.info(f"Request headers: {prepared_request.headers}")
                
                resp = self._session.send(prepared_request, timeout=self.request_timeout)
                self.logger.info(f"Response received with status code: {resp.status_code}")
                
                if resp.status_code != 200:
                    self.logger.error(f"Error response from API: {resp.text}")
                    if resp.status_code == 504:
                        if retry_count < self.max_retries:
                            retry_count += 1
                            wait_time = self.retry_backoff_factor * (2 ** (retry_count - 1))
                            self.logger.info(f"Gateway timeout. Retrying in {wait_time} seconds... (Attempt {retry_count}/{self.max_retries})")
                            time.sleep(wait_time)
                            continue
                    raise requests.exceptions.HTTPError(f"HTTP {resp.status_code}: {resp.text}")
                
                records = list(self.parse_response(resp))
                self.logger.info(f"Parsed {len(records)} records from response")
                
                # Update max_updated_at if we have records
                if records:
                    for record in records:
                        # Try to get updatedAt from attributes first
                        updated_at = (record.get('attributes', {}) or {}).get('updatedAt')
                        if not updated_at:
                            # If not in attributes, try root level
                            updated_at = record.get('updatedAt')
                        
                        if updated_at:
                            if max_updated_at is None or updated_at > max_updated_at:
                                max_updated_at = updated_at

                # Process and yield records
                for record in records:
                    processed_record = self.post_process(record, context)
                    if processed_record:
                        yield processed_record

                next_page_token = self.get_next_page_token(resp, next_page_token)
                if not next_page_token:
                    finished = True
                    
            except requests.exceptions.Timeout as e:
                self.logger.error(f"Request timed out: {str(e)}")
                if retry_count < self.max_retries:
                    retry_count += 1
                    wait_time = self.retry_backoff_factor * (2 ** (retry_count - 1))
                    self.logger.info(f"Request timed out. Retrying in {wait_time} seconds... (Attempt {retry_count}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                self.logger.error(f"Error during record retrieval: {str(e)}")
                raise
                
        # Update state with the maximum updatedAt value after all records are processed
        if max_updated_at:
            state_record = {'attributes': {'updatedAt': max_updated_at}}
            self._increment_stream_state(state_record, context=context)
