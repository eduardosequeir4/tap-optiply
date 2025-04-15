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
        self.api = api
        self.authenticator = OptiplyAuthenticator.create_for_stream(self)

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

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a request to the Optiply API."""
        url = f"{self.url_base}{path}"
        headers = self.authenticator.auth_headers

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

    def get_url_params(
        self, context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get URL parameters for the request."""
        params = super().get_url_params(context)
        
        # Add account_id filter if available
        if self.api and hasattr(self.api, 'account_id') and self.api.account_id:
            params["filter[accountId]"] = self.api.account_id
            
        # Add replication key if available
        if self.replication_key:
            params["filter[updatedAt][GT]"] = self.get_starting_timestamp(context)
            
        return params

class OptiplyAPI:
    """Optiply API client."""

    def __init__(self, config: dict) -> None:
        """Initialize the API client.

        Args:
            config: Configuration dictionary.
        """
        self.config = config
        self._access_token = None
        self._account_id = None
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
        })
        
        # Extract credentials from config
        self.username = config.get("username")
        self.password = config.get("password")
        self.client_id = config.get("client_id")
        self.client_secret = config.get("client_secret")
        self.account_id = config.get("account_id")
        
        # Create Basic Auth header for token requests - using the exact format from the example
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        self.auth_header = f"Basic {encoded_credentials}"
        
        # Set the base URL for API requests
        self.base_url = "https://dashboard.optiply.nl/api"
        
        # Get initial token
        self._get_access_token()

    def _get_access_token(self):
        """Get OAuth access token using client credentials flow."""
        try:
            # Construct the token URL with grant_type as a query parameter
            token_url = f"{self.base_url}/auth/oauth/token?grant_type=password"
            
            # Create Basic Auth header using client_id and client_secret
            auth_string = f"{self.client_id}:{self.client_secret}"
            auth_bytes = auth_string.encode('ascii')
            base64_auth = base64.b64encode(auth_bytes).decode('ascii')
            
            # Set up headers and data
            headers = {
                'Authorization': f'Basic {base64_auth}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'username': self.username,
                'password': self.password
            }
            
            # Make the token request
            response = requests.post(
                token_url,
                headers=headers,
                data=data
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse the response
            token_data = response.json()
            
            # Validate required fields
            if 'access_token' not in token_data:
                raise ValueError("Access token not found in response")
                
            # Store the token and its expiration
            self._access_token = token_data['access_token']
            if 'expires_in' in token_data:
                self.token_expires_at = time.time() + token_data['expires_in']
            
            # Update session headers with the new token
            self._session.headers.update({"Authorization": f"Bearer {self._access_token}"})
            
            logger.info("Successfully obtained new access token")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to obtain access token: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response content: {e.response.text}")
            raise

    def _get_account_id(self) -> int:
        """Get the account ID from the API.

        Returns:
            The account ID.
        """
        if self._account_id:
            return self._account_id

        url = "https://api.optiply.com/v1/accounts"
        response = self._session.get(url)
        response.raise_for_status()
        self._account_id = response.json()["data"][0]["id"]
        return self._account_id

    @retry_with_backoff(max_retries=3, initial_delay=1.0)
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an HTTP request with retry logic and timeout.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL to make the request to
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response object

        Raises:
            RequestException: If the request fails after all retries
        """
        # Set default timeout if not provided
        if 'timeout' not in kwargs:
            # For GET requests to endpoints that return large responses, use a longer read timeout
            if method == "GET" and any(endpoint in url for endpoint in ["/sellOrderLines", "/products"]):
                kwargs['timeout'] = (5, 120)  # (connect timeout, read timeout)
            else:
                kwargs['timeout'] = (5, 30)  # (connect timeout, read timeout)
            
        # Set default pagination parameters for GET requests
        if method == "GET":
            params = kwargs.get('params', {})
            if params is None:
                params = {}
            if not any(key.startswith('page[') for key in params):
                params['page[limit]'] = 100  # Larger page size to increase efficiency
            kwargs['params'] = params
                
        # Log detailed request information
        full_url = url
        if 'params' in kwargs and kwargs['params']:
            # Construct full URL with query parameters for logging
            query_string = urllib.parse.urlencode(kwargs['params'], doseq=True)
            full_url = f"{url}?{query_string}"
            
        logger.info(f"Making {method} request to: {full_url}")
        logger.info(f"Request parameters: {kwargs.get('params', {})}")
        
        # Log headers (excluding sensitive information)
        if 'headers' in kwargs:
            headers = kwargs['headers'].copy()
            if 'Authorization' in headers:
                headers['Authorization'] = 'Bearer [REDACTED]'
            logger.info(f"Request headers: {headers}")
                
        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
            
            # Log response information
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            # Check for empty responses
            if response.status_code == 204:
                logger.debug(f"Received empty response (204) from {full_url}")
                return response
                
            # Check for valid JSON response
            try:
                json_response = response.json()
                # Log response size and record count if available
                if isinstance(json_response, dict):
                    if 'data' in json_response:
                        record_count = len(json_response['data'])
                        logger.info(f"Response contains {record_count} records")
                    if 'meta' in json_response and 'total' in json_response['meta']:
                        logger.info(f"Total records available: {json_response['meta']['total']}")
            except ValueError as e:
                logger.error(f"Invalid JSON response from {full_url}: {str(e)}")
                logger.error(f"Response content: {response.text[:200]}...")
                raise HTTPError(f"Invalid JSON response: {str(e)}")
                
            return response
            
        except HTTPError as e:
            if e.response.status_code == 401:  # Unauthorized
                logger.error(f"Authentication failed for {full_url}")
                raise
            elif e.response.status_code == 403:  # Forbidden
                logger.error(f"Access forbidden for {full_url}")
                raise
            elif e.response.status_code == 404:  # Not Found
                logger.error(f"Resource not found at {full_url}")
                raise
            elif e.response.status_code == 500:  # Internal Server Error
                logger.error(f"Server error for {full_url}")
                raise
            else:
                logger.error(f"HTTP error {e.response.status_code} for {full_url}")
                logger.error(f"Response content: {e.response.text[:200]}...")
                raise
        except Timeout as e:
            logger.error(f"Request timed out for {full_url}")
            raise
        except RequestException as e:
            logger.error(f"Request failed for {full_url}: {str(e)}")
            raise

    def get_records(self, stream_name: str, params: t.Optional[dict] = None) -> t.Iterable[dict]:
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

        while True:
            response = self._make_request("GET", url, params=params)
            data = response.json()

            for record in data["data"]:
                yield record

            # Check for pagination links
            if "links" in data and "next" in data["links"]:
                url = data["links"]["next"]
                params = None  # Clear params as they're included in the next URL
            else:
                break
