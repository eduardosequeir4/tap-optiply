"""Optiply Authentication."""

import json
import base64
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import backoff
import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import RESTStream


class OptiplyAuthenticator(APIAuthenticatorBase):
    """Authenticator for Optiply API."""

    def __init__(
        self,
        stream: RESTStream,
        auth_endpoint: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
    ) -> None:
        """Initialize the authenticator.

        Args:
            stream: The stream instance.
            auth_endpoint: The authentication endpoint URL.
            client_id: The client ID.
            client_secret: The client secret.
            username: The username.
            password: The password.
        """
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._password = password
        self._access_token = None
        self._token_expires_at = None
        self._load_token_from_config()

    def _load_token_from_config(self) -> None:
        """Load token and expiration from config if available."""
        if "access_token" in self._config and "token_expires_at" in self._config:
            self._access_token = self._config["access_token"]
            self._token_expires_at = datetime.fromisoformat(self._config["token_expires_at"])
            self.auth_headers = {"Authorization": f"Bearer {self._access_token}"}
            self.logger.info("Loaded token from config")

    def _save_token_to_config(self) -> None:
        """Save token and expiration to config."""
        self._config["access_token"] = self._access_token
        self._config["token_expires_at"] = self._token_expires_at.isoformat()
        self.logger.info("Saved token to config")

    def is_token_valid(self) -> bool:
        """Check if the current token is valid.

        Returns:
            bool: True if the token is valid, False otherwise.
        """
        if not self._access_token or not self._token_expires_at:
            return False
        return datetime.now() < self._token_expires_at - timedelta(seconds=120)

    def update_access_token(self) -> None:
        """Update the access token."""
        auth_string = f"{self._client_id}:{self._client_secret}"
        auth_bytes = auth_string.encode()
        auth_b64 = base64.b64encode(auth_bytes).decode()

        headers = {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "username": self._username,
            "password": self._password,
        }

        try:
            response = requests.post(
                f"{self._auth_endpoint}?grant_type=password",
                headers=headers,
                data=data,
            )
            response.raise_for_status()
            auth_response = response.json()

            self._access_token = auth_response["access_token"]
            expires_in = auth_response.get("expires_in", 3600)
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            self.auth_headers = {"Authorization": f"Bearer {self._access_token}"}
            self._save_token_to_config()
            self.logger.info("Successfully updated access token")
        except Exception as e:
            self.logger.error(f"Failed to update access token: {str(e)}")
            raise

    @classmethod
    def create_for_stream(
        cls,
        stream: RESTStream,
        auth_endpoint: Optional[str] = None,
    ) -> "OptiplyAuthenticator":
        """Create an authenticator for the given stream.

        Args:
            stream: The stream to create the authenticator for.
            auth_endpoint: The authentication endpoint URL.

        Returns:
            OptiplyAuthenticator: The created authenticator.
        """
        config = stream._tap.config
        return cls(
            stream=stream,
            auth_endpoint=auth_endpoint or "https://dashboard.optiply.nl/api/auth/oauth/token",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            username=config["username"],
            password=config["password"],
        ) 