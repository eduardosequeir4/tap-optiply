"""Custom client handling for tap-tapoptiply."""

from __future__ import annotations

import typing as t
import time
import requests
import urllib.parse
import logging
from functools import wraps
from requests.exceptions import RequestException, Timeout, HTTPError
import datetime
import base64
from typing import Any, Dict, Optional
from singer_sdk.streams import Stream as RESTStreamBase
from tap_optiply.auth import OptiplyAuthenticator
import json
import os

# Set up logging
logging.basicConfig(level=logging.DEBUG)  # Set root logger to DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set logger level to DEBUG

# Add a handler if none exists
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0, max_504_retries: int = 2):
    """Decorator to retry a function with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries for general errors.
        initial_delay: Initial delay in seconds.
        max_504_retries: Maximum number of retries for 504 Gateway Timeout errors.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            retry_count = 0
            gateway_timeout_count = 0
            
            while True:
                try:
                    return func(*args, **kwargs)
                except HTTPError as e:
                    last_exception = e
                    if e.response.status_code == 504:  # Gateway Timeout
                        gateway_timeout_count += 1
                        if gateway_timeout_count <= max_504_retries:
                            logger.warning(f"504 Gateway Timeout (attempt {gateway_timeout_count}/{max_504_retries}). Retrying in {delay} seconds...")
                            time.sleep(delay)
                            delay *= 2  # Exponential backoff
                            continue
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Attempt {retry_count} failed: {str(e)}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                        continue
                except Timeout as e:
                    last_exception = e
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Attempt {retry_count} failed: {str(e)}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                        continue
                
                logger.error(f"All retries failed. Last error: {str(last_exception)}")
                raise last_exception
        return wrapper
    return decorator

class OptiplyStream(RESTStreamBase):
    """Optiply stream class."""

    url_base = "https://api.optiply.com/v1"

    def __init__(self, tap: t.Any, api: t.Optional[OptiplyAPI] = None, **kwargs):
        """Initialize the stream.

        Args:
            tap: The parent tap object.
            api: Optional API client instance.
            **kwargs: Additional keyword arguments to pass to the parent class.
        """
        super().__init__(tap=tap, **kwargs)
        self.api = api or OptiplyAPI(tap.config, tap=tap)
        self.authenticator = OptiplyAuthenticator(tap.config, tap=tap)

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

    def get_url_params(
        self,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get URL parameters for the request."""
        params = super().get_url_params(context)
        
        # Add account_id filter from config
        if self._tap.config.get("account_id"):
            params["filter[accountId]"] = self._tap.config["account_id"]
            
        # Add replication key if available
        if self.replication_key:
            params[f"filter[{self.replication_key}][GT]"] = self.get_starting_timestamp(context)
            
        # Set page limit
        params["page[limit]"] = 100
            
        return params

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a request to the Optiply API."""
        url = f"{self.url_base}{path}"
        headers = self.authenticator.get_auth_headers()

        logger.info(f"Making {method} request to {url}")
        if params:
            logger.info(f"Request params: {params}")
        if json:
            logger.info(f"Request body: {json}")

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            logger.error(f"Response content: {response.text}")
            raise

        return response.json()

class OptiplyAPI:
    """Optiply API client."""

    def __init__(this, config: dict, tap: Optional[Any] = None) -> None:
        """Initialize the API client.

        Args:
            config: Configuration dictionary.
            tap: Optional tap instance.
        """
        this.config = config
        this._tap = tap
        this.session = requests.Session()
        this.authenticator = OptiplyAuthenticator(config, tap=tap)
        this.session.headers.update({
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
        })
        this.session.headers.update(this.authenticator.get_auth_headers())

    def _make_request(self, method: str, url: str, **kwargs) -> dict:
        """Make a request to the API.

        Args:
            method: HTTP method.
            url: URL to request.
            **kwargs: Additional arguments to pass to requests.

        Returns:
            dict: Response data.
        """
        # Update headers with latest auth token
        self.session.headers.update(self.authenticator.get_auth_headers())
        
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def get(self, url: str, **kwargs) -> dict:
        """Make a GET request.

        Args:
            url: URL to request.
            **kwargs: Additional arguments to pass to requests.

        Returns:
            dict: Response data.
        """
        return self._make_request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> dict:
        """Make a POST request.

        Args:
            url: URL to request.
            **kwargs: Additional arguments to pass to requests.

        Returns:
            dict: Response data.
        """
        return self._make_request("POST", url, **kwargs)

    def get_records(this, stream_name: str, params: t.Optional[dict] = None) -> t.Iterable[dict]:
        """Get records from a stream.

        Args:
            stream_name: The name of the stream.
            params: Optional parameters to include in the request.

        Yields:
            Records from the stream.
        """
        # Convert stream_name to path by converting snake_case to camelCase
        parts = stream_name.split('_')
        path = parts[0] + ''.join(word.capitalize() for word in parts[1:])
        url = f"https://api.optiply.com/v1/{path}"

        # Ensure accountId is in params if available
        if this.config.get("account_id") and params is not None:
            params['filter[accountId]'] = this.config["account_id"]

        while True:
            response = this._make_request("GET", url, params=params)
            data = response

            for record in data["data"]:
                yield record

            # Check for pagination links
            if "links" in data and "next" in data["links"]:
                url = data["links"]["next"]
                # Parse the next URL to preserve accountId
                parsed_url = urllib.parse.urlparse(data["links"]["next"])
                query_params = dict(urllib.parse.parse_qsl(parsed_url.query))
                if this.config.get("account_id"):
                    query_params['filter[accountId]'] = this.config["account_id"]
                params = query_params
            else:
                break
