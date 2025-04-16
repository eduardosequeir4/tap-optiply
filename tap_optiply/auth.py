"""Authentication handler for Optiply API."""
import base64
import json
import argparse
from datetime import datetime
from typing import Dict, Optional

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk._singerlib import Message


class OptiplyAuthenticator(OAuthAuthenticator):
    """Authenticator class for Optiply API."""

    # Fixed token URL that will never change
    TOKEN_URL = "https://dashboard.optiply.nl/api/auth/oauth/token"
    
    # Store token in class variables
    _access_token = None
    _token_expires_at = None

    def __init__(
        self,
        stream: RESTStream,
    ) -> None:
        """Initialize the authenticator.

        Args:
            stream: The stream instance
        """
        self._stream = stream
        super().__init__(stream=stream)
        
        # Initialize token from config if available
        self._access_token = stream.config.get("access_token")
        self._token_expires_at = stream.config.get("token_expires_at")

    def update_config(self, new_fields: Dict[str, str]) -> None:
        """Update the config.

        Args:
            new_fields: Dictionary of new fields to update in the config
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', '--config', help='Config file', required=True)
        _args, unknown = parser.parse_known_args()
        config_file = _args.config
        with open(f"{config_file}", 'r') as filetoread:
            data = filetoread.read()
        self.logger.info(f"Config file: {data}")
        config = json.loads(data)
        config.update(new_fields)
        self.logger.info(f"Config: {config}")
        with open(f"{config_file}", 'w') as filetowrite:
            json.dump(config, filetowrite)

    @property
    def stream(self) -> RESTStream:
        """Get the stream instance.

        Returns:
            The stream instance.
        """
        return self._stream

    def authenticate_request(self, request):
        """Authenticate the request."""
        headers = self.get_auth_headers()
        request.headers.update(headers)
        return request

    def get_auth_headers(self) -> Dict[str, str]:
        """Get the authorization headers.

        Returns:
            A dictionary containing the authorization headers.
        """
        if not self.is_token_valid():
            self.update_access_token()

        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/vnd.api+json",
        }

    def is_token_valid(self) -> bool:
        """Check if the current access token is valid.

        Returns:
            True if the token is valid, False otherwise.
        """
        if not self._access_token:
            return False

        if not self._token_expires_at:
            return False

        now = round(datetime.utcnow().timestamp())
        return not ((self._token_expires_at - now) < 120)

    def update_access_token(self) -> None:
        """Update the access token using the OAuth credentials."""
        # Create Basic Auth header
        auth_string = f"{self.stream.config['client_id']}:{self.stream.config['client_secret']}"
        auth_bytes = auth_string.encode("ascii")
        base64_auth = base64.b64encode(auth_bytes).decode("ascii")

        headers = {
            "Authorization": f"Basic {base64_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "password",
            "username": self.stream.config["username"],
            "password": self.stream.config["password"],
        }

        token_response = requests.post(
            self.TOKEN_URL,
            headers=headers,
            data=data,
            timeout=30,
        )

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )

        token_json = token_response.json()

        # Update the class variables with new token
        self._access_token = token_json["access_token"]
        now = round(datetime.utcnow().timestamp())
        self._token_expires_at = int(token_json["expires_in"]) + now
        
        # Update config with new token and expiration
        self.update_config({
            "access_token": self._access_token,
            "token_expires_at": self._token_expires_at
        })

        # Save state and log token
        self.logger.info(f"New access token obtained: {self._access_token}") 