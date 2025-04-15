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

# Set up logging
logger = logging.getLogger(__name__)

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

class OptiplyAPI:
    """Optiply API client."""

    def __init__(self, username: str, password: str, account_id: int, config: dict) -> None:
        """Initialize the Optiply API client.

        Args:
            username: The username for authentication.
            password: The password for authentication.
            account_id: The account ID to filter requests.
            config: The tap configuration dictionary.
        """
        self.base_url = "https://api.optiply.com/v1"
        self.session = requests.Session()
        self._username = username
        self._password = password
        self._account_id = account_id
        self._auth_url = "https://dashboard.optiply.nl/api/auth/oauth/token"
        
        # Get configuration from either nested 'config' object or root level
        self._config = config.get("config", config)
        
        # Get token information
        self._token = self._config.get("access_token")
        self._refresh_token = self._config.get("refresh_token")
        self._token_expires_at = self._config.get("token_expires_at")
        self._token_type = None
        self._scope = None
        
        # Get client credentials
        client_id = self._config.get("client_id")
        client_secret = self._config.get("client_secret")
        
        # Create Basic Auth header
        if client_id and client_secret:
            auth_string = f"{client_id}:{client_secret}"
            auth_bytes = auth_string.encode('ascii')
            base64_auth = base64.b64encode(auth_bytes).decode('ascii')
            self._authorization = f"Basic {base64_auth}"
        else:
            raise ValueError("client_id and client_secret are required for Basic Authentication")
        
        # Set default headers
        self.session.headers.update({
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
            "Authorization": self._authorization,
        })
        
        # Get initial token if not present or expired
        if not self._token or not self._token_expires_at or time.time() >= self._token_expires_at:
            self._update_token()
            
        # Update authorization header with token after getting it
        if self._token and self._token_type:
            self.session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._token}"

    def get_records(self, stream_name: str, params: dict) -> t.Iterable[dict]:
        """Get records for a given stream.

        Args:
            stream_name: The name of the stream to get records for.
            params: Query parameters.

        Yields:
            Records from the API.
        """
        # Get the stream class from STREAM_TYPES
        from tap_optiply.streams import STREAM_TYPES
        stream_class = STREAM_TYPES.get(stream_name)
        
        # Use the stream's path if available, otherwise convert snake_case to camelCase
        if stream_class and hasattr(stream_class, 'path'):
            endpoint = stream_class.path
            logger.info(f"Using stream path for {stream_name}: {endpoint}")
        else:
            parts = stream_name.split('_')
            endpoint = parts[0] + ''.join(word.capitalize() for word in parts[1:])
            logger.info(f"Using converted path for {stream_name}: {endpoint}")
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Making request to: {url}")
        
        # Set default page size if not provided
        if "page[limit]" not in params:
            params["page[limit]"] = 100
            
        # Initialize offset if not provided
        if "page[offset]" not in params:
            params["page[offset]"] = 0

        # Add account_id filter to params
        request_params = self._get_default_params()
        request_params.update(params)
            
        while True:
            response = self._make_request("GET", url, params=request_params)
            data = response.json()
            
            for record in data.get("data", []):
                yield record
            
            # Check for pagination using the links format from the API
            links = data.get("links", {})
            if "next" not in links:
                break
                
            # Extract the next URL and parse it to get the new parameters
            next_url = links["next"]
            parsed_url = urllib.parse.urlparse(next_url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Update params with the new values from the next URL
            for key, value in query_params.items():
                request_params[key] = value[0]  # Take the first value from the list

    def _create_basic_auth_header(self) -> str:
        """Create Basic Auth header for token requests.

        Returns:
            Basic Auth header string.
        """
        auth_string = f"{self._config.get('client_id')}:{self._config.get('client_secret')}"
        auth_bytes = auth_string.encode('ascii')
        base64_auth = base64.b64encode(auth_bytes).decode('ascii')
        return f"Basic {base64_auth}"

    def _update_token_info(self, token_data: dict) -> None:
        """Update token information from response data.

        Args:
            token_data: Token data from API response.
        """
        self._token = token_data["access_token"]
        self._refresh_token = token_data.get("refresh_token", self._refresh_token)
        self._token_type = token_data["token_type"]
        self._scope = token_data["scope"]
        # Set expiration time (subtract 60 seconds for safety margin)
        self._token_expires_at = time.time() + token_data["expires_in"] - 60
        
        # Update config with new token information
        if "apiCredentials" not in self._config:
            self._config["apiCredentials"] = {}
        self._config["apiCredentials"].update({
            "access_token": self._token,
            "refresh_token": self._refresh_token,
            "token_expires_at": self._token_expires_at,
        })
        
        logger.info(f"Got access token: {self._token[:10]}...")
        logger.debug(f"Token type: {self._token_type}")
        
        # Update authorization header with Bearer token
        self.session.headers["Authorization"] = f"Bearer {self._token}"
        logger.debug(f"Updated session headers: {self.session.headers}")

    def _update_token(self) -> None:
        """Update the access token."""
        logger.info("Attempting to get access token...")
        
        auth_response = self._make_request(
            "POST",
            self._auth_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": self._create_basic_auth_header(),
            },
            data={
                "grant_type": "password",
                "username": self._username,
                "password": self._password,
            },
        )
        
        self._update_token_info(auth_response.json())

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                logger.info("Attempting to refresh token...")
                
                auth_response = self._make_request(
                    "POST",
                    self._auth_url,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Authorization": self._create_basic_auth_header(),
                    },
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._refresh_token,
                    },
                )
                
                self._update_token_info(auth_response.json())
            else:
                self._update_token()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _get_default_params(self) -> dict:
        """Get default parameters including account_id filter.

        Returns:
            Default parameters dictionary.
        """
        return {"filter[accountId]": self._account_id}

    def _get_paginated_records(self, endpoint: str, params: dict | None = None, record_type: str = "records") -> t.Iterator[dict]:
        """Get paginated records from Optiply API.

        Args:
            endpoint: API endpoint path (e.g. 'products', 'buyOrders')
            params: Optional query parameters
            record_type: Description of the record type for logging (e.g. "products", "buy orders")

        Yields:
            API records
        """
        url = f"{self.base_url}/{endpoint}"
        page_size = 100
        offset = 0
        total_records = 0

        while True:
            try:
                # Start with default params including account_id
                current_params = self._get_default_params()
                # Update with user params if provided
                if params:
                    current_params.update(params)
                current_params.update({
                    "page[limit]": page_size,
                    "page[offset]": offset,
                })

                response = self._make_request("GET", url, params=current_params)
                data = response.json()
                records = data.get("data", [])
                total_records += len(records)
                logger.info(f"Retrieved {len(records)} {record_type} (total: {total_records})")

                for record in records:
                    yield record

                if len(records) < page_size:
                    break

                offset += page_size

            except (Timeout, HTTPError) as e:
                logger.error(f"Error fetching {record_type}: {str(e)}")
                raise

    def get_products(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get products from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Product records.
        """
        return self._get_paginated_records("products", params, "products")

    def get_suppliers(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get suppliers from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Supplier records.
        """
        return self._get_paginated_records("suppliers", params, "suppliers")

    def get_supplier_products(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get supplier products from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Supplier product records.
        """
        return self._get_paginated_records("supplierProducts", params, "supplier products")

    def get_buy_orders(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get buy orders from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Buy order records.
        """
        return self._get_paginated_records("buyOrders", params, "buy orders")

    def get_sell_orders(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get sell orders from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Sell order records.
        """
        return self._get_paginated_records("sellOrders", params, "sell orders")

    def get_buy_order_lines(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get buy order lines from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Buy order line records.
        """
        return self._get_paginated_records("buyOrderLines", params, "buy order lines")

    def get_sell_order_lines(self, params: dict | None = None) -> t.Iterator[dict]:
        """Get sell order lines from Optiply API.

        Args:
            params: Optional query parameters.

        Yields:
            Sell order line records.
        """
        return self._get_paginated_records("sellOrderLines", params, "sell order lines")

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
        if method == "GET" and 'params' in kwargs:
            params = kwargs['params']
            if not any(key.startswith('page[') for key in params):
                params['page[limit]'] = 100  # Larger page size to increase efficiency
                
        # Log detailed request information
        logger.info(f"Making {method} request to {url}")
        if 'params' in kwargs:
            logger.info(f"Request parameters: {kwargs['params']}")
        if 'headers' in kwargs:
            # Mask sensitive headers
            headers = kwargs['headers'].copy()
            if 'Authorization' in headers:
                headers['Authorization'] = 'Bearer [REDACTED]'
            logger.info(f"Request headers: {headers}")
                
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            
            # Check for empty responses
            if response.status_code == 204:
                logger.debug(f"Received empty response (204) from {url}")
                return response
                
            # Check for valid JSON response
            try:
                response.json()
            except ValueError as e:
                logger.error(f"Invalid JSON response from {url}: {str(e)}")
                logger.error(f"Response content: {response.text[:200]}...")
                raise HTTPError(f"Invalid JSON response: {str(e)}")
                
            return response
            
        except HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                logger.error(f"Rate limit exceeded for {url}")
                raise
            elif e.response.status_code == 401:  # Unauthorized
                logger.error(f"Authentication failed for {url}")
                raise
            elif e.response.status_code == 403:  # Forbidden
                logger.error(f"Access forbidden for {url}")
                raise
            elif e.response.status_code == 404:  # Not Found
                logger.error(f"Resource not found at {url}")
                raise
            elif e.response.status_code == 500:  # Internal Server Error
                logger.error(f"Server error for {url}")
                raise
            else:
                logger.error(f"HTTP error {e.response.status_code} for {url}")
                logger.error(f"Response content: {e.response.text[:200]}...")
                raise
        except Timeout as e:
            logger.error(f"Request timed out for {url}")
            raise
        except RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise
