"""Optiply Authentication."""

import json
import base64
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union

import backoff
import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import Stream as RESTStreamBase


class OptiplyAuthenticator:
    """Authenticator for Optiply API."""

    def __init__(
        self,
        config: dict,
        tap: Optional[Any] = None,
        auth_endpoint: str = "https://dashboard.optiply.nl/api/auth/oauth/token",
    ) -> None:
        """Initialize the authenticator.

        Args:
            config: Configuration dictionary.
            tap: Optional tap instance.
            auth_endpoint: The authentication endpoint URL.
        """
        self.config = dict(config)  # Make a copy of the config
        self._auth_endpoint = auth_endpoint
        self._access_token = None
        self._token_expires_at = None
        self._tap = tap
        self._load_token_from_config()

    def _load_token_from_config(self) -> bool:
        """Load token from config if it exists."""
        if 'access_token' in self.config and 'token_expires_at' in self.config:
            self._access_token = self.config['access_token']
            self._token_expires_at = self.config['token_expires_at']
            return True
        return False

    def _save_token_to_config(self) -> None:
        """Save token to config."""
        self.config['access_token'] = self._access_token
        self.config['token_expires_at'] = self._token_expires_at
        
        # Save to config file if tap instance is available
        if hasattr(self, '_tap') and self._tap and hasattr(self._tap, 'config_file'):
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self.config, outfile, indent=4)

    def _is_token_valid(self) -> bool:
        """Check if the current token is valid."""
        if not self._access_token or not self._token_expires_at:
            return False
        try:
            now = round(datetime.utcnow().timestamp())
            expires_in = int(self._token_expires_at)
            return not ((expires_in - now) < 120)
        except (ValueError, TypeError):
            return False

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
    def update_access_token(self) -> None:
        """Update the access token."""
        # Create Basic Auth header using client_id and client_secret
        auth_string = f"{self.config['client_id']}:{self.config['client_secret']}"
        auth_bytes = auth_string.encode('ascii')
        base64_auth = base64.b64encode(auth_bytes).decode('ascii')
        
        headers = {
            'Authorization': f'Basic {base64_auth}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'username': self.config['username'],
            'password': self.config['password']
        }

        try:
            response = requests.post(
                f"{self._auth_endpoint}?grant_type=password",
                headers=headers,
                data=data
            )
            response.raise_for_status()
            auth_response = response.json()

            # Update tokens
            self._access_token = auth_response["access_token"]
            
            # Update expiration
            expires_in = auth_response.get("expires_in", 3600)
            now = round(datetime.utcnow().timestamp())
            self._token_expires_at = int(expires_in) + now
            
            # Save to config
            self._save_token_to_config()
            
        except Exception as e:
            print(f"Failed to update access token: {str(e)}")
            raise

    def get_auth_headers(self) -> Dict[str, str]:
        """Get the authentication headers.

        Returns:
            The authentication headers.
        """
        if not self._is_token_valid():
            self.update_access_token()
        return {"Authorization": f"Bearer {self._access_token}"} 