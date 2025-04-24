"""Custom client handling, including OptiplyStream base class."""

from __future__ import annotations

import typing as t
from datetime import datetime
from urllib.parse import parse_qsl, urlparse
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_optiply.auth import OptiplyAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.typing import Context


class OptiplyStream(RESTStream):
    """Stream class for Optiply streams."""

    # Default page size
    page_size = 100

    # Default HTTP method
    http_method = "GET"

    # Required headers
    http_headers = {"Content-Type": "application/vnd.api+json"}

    # Base URL
    url_base = "https://api.optiply.com/v1"

    # Timeout settings (in seconds)
    request_timeout = 60  # 60 seconds default timeout for all streams
    max_retries = 3
    retry_backoff_factor = 1
    retry_status_forcelist = [408, 429, 500, 502, 503, 504]

    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self._authenticator = None
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=self.retry_status_forcelist,
        )
        
        # Create a session with the retry strategy
        self._session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    @property
    def authenticator(self) -> OAuthAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        if not self._authenticator:
            self._authenticator = OptiplyAuthenticator(
                stream=self,
            )

        return self._authenticator

    def get_new_paginator(self):
        """Get a fresh paginator for this API endpoint.

        Returns:
            A paginator instance.
        """
        return None

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token for next page of results.

        Returns:
            Dictionary of URL query parameters.
        """
        # Get pagination parameters from parent class
        params = super().get_url_params(context, next_page_token)

        # Add page limit parameter
        params["page[limit]"] = self.page_size

        # Add account ID filter
        params["filter[accountId]"] = self.config["account_id"]

        # Get state for replication key
        state = self.get_context_state(context)
        replication_key_value = None

        if state:
            # Try to get the value from state first
            replication_key_value = state.get(self.replication_key)
            if replication_key_value:
                self.logger.info(f"Using state value for {self.replication_key}: {replication_key_value}")
                params["filter[updatedAt][GT]"] = replication_key_value.replace("Z", "+00:00")
            else:
                # Fall back to start_date from config if no state
                start_date = self.config.get("start_date")
                if start_date:
                    self.logger.info(f"Using start_date from config: {start_date}")
                    params["filter[updatedAt][GT]"] = start_date.replace("Z", "+00:00")

        # Log the final parameters for debugging
        self.logger.info("Request parameters: %s", params)

        return params

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
                
                if resp.status_code == 401:
                    self.logger.info("Received 401 error, attempting to refresh token...")
                    # Force token refresh by clearing the current token
                    self.authenticator._access_token = None
                    if retry_count < self.max_retries:
                        retry_count += 1
                        continue
                    else:
                        self.logger.error("Max retries reached for token refresh")
                        raise requests.exceptions.HTTPError(f"HTTP {resp.status_code}: {resp.text}")
                elif resp.status_code != 200:
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

    def prepare_request(
        self,
        context: dict | None,
        next_page_token: t.Any | None = None,
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token for next page of results.

        Returns:
            A prepared request object.
        """
        # If we have a full URL from next_page_token, use it directly
        if isinstance(next_page_token, str) and next_page_token.startswith("http"):
            url = next_page_token
            params = {}
        else:
            url = self.get_url(context)
            params = self.get_url_params(context, next_page_token)

        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            auth_headers = authenticator.get_auth_headers()
            self.logger.info("Auth headers: %s", auth_headers)
            headers.update(auth_headers)

        request = requests.Request(
            method=self.http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )
        
        # Log the full request details
        self.logger.info("Full request URL: %s", request.url)
        self.logger.info("Request headers: %s", headers)
        self.logger.info("Request params: %s", params)
        
        return request.prepare()

    def get_replication_key_value(self, record: dict) -> str | None:
        """Return the value of the replication key from the record.

        Args:
            record: The record to get the replication key value from.

        Returns:
            The value of the replication key.
        """
        # Try to get the value from attributes first
        if 'attributes' in record:
            value = record['attributes'].get(self.replication_key)
            if value is not None:
                return value

        # If not in attributes, try root level
        return record.get(self.replication_key)

    def _increment_stream_state(self, latest_record: dict, *, context: dict | None = None) -> None:
        """Update state of stream or partition with data from the provided record.

        Args:
            latest_record: Latest record to use for updating state.
            context: Stream partition or context dictionary.
        """
        state = self.get_context_state(context) or {}
        replication_key_value = self.get_replication_key_value(latest_record)

        if replication_key_value is None:
            return

        current_value = state.get(self.replication_key)
        if not current_value or replication_key_value > current_value:
            state[self.replication_key] = replication_key_value
            self.state = state

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,
    ) -> t.Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: API response object.
            previous_token: Previous page token.

        Returns:
            Next page token or None if no more pages.
        """
        data = response.json()
        
        # If we have a next link, use it directly
        if "links" in data and "next" in data["links"] and data["links"]["next"]:
            return data["links"]["next"]
            
        return None

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The response from the API.

        Yields:
            Each record from the source.
        """
        self.logger.info(f"Response status code: {response.status_code}")
        self.logger.info(f"Response headers: {response.headers}")
        self.logger.info(f"Response text: {response.text[:1000]}")  # Log first 1000 chars
        
        try:
            data = response.json()
            records = data.get("data", [])
            for record in records:
                yield record
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            self.logger.error(f"Response text: {response.text}")
            raise

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process the record.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The processed record.
        """
        if not row:
            return None

        # Start with the basic fields
        processed_row = {
            "id": row.get("id"),
            "type": row.get("type")
        }
        
        # Add all attributes to root level
        if "attributes" in row:
            processed_row.update(row["attributes"])

        return processed_row
