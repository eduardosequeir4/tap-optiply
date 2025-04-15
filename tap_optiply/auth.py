"""Optiply Authentication."""

import json
from datetime import datetime
from typing import Optional

import backoff
import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import Stream as RESTStreamBase


class OptiplyAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Optiply."""

    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
    ) -> None:
        super().__init__(
            stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes
        )
        self._config_file = config_file
        self._tap = stream._tap

    @property
    def auth_headers(self) -> dict:
        """Get auth headers."""
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self._tap._config.get('access_token')}"
        return result

    def is_token_valid(self) -> bool:
        """Check if the token is still valid."""
        access_token = self._tap._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._tap.config.get("expires_in")

        if expires_in is not None:
            expires_in = int(expires_in)

        if not access_token:
            return False

        if not expires_in:
            return False

        # Return False if token expires in less than 120 seconds
        return not ((expires_in - now) < 120)

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Optiply API."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> "OptiplyAuthenticator":
        """Create an authenticator for the given stream."""
        return cls(
            stream=stream,
            auth_endpoint="https://api.optiply.com/oauth/token",
            oauth_scopes="",
        )

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        """Update the access token."""
        self.logger.info(
            f"OAuth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}"
        )
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_response = requests.post(
            self._auth_endpoint,
            data=self.oauth_request_body,
            headers=headers
        )

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            )

        token_json = token_response.json()

        # Store the access token and expiration
        self.access_token = token_json["access_token"]
        self._tap._config["access_token"] = token_json["access_token"]
        now = round(datetime.utcnow().timestamp())
        self._tap._config["expires_in"] = now + token_json["expires_in"]

        # Save the updated config
        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4) 