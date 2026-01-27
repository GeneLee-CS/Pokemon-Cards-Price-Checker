"""
eBay OAuth authentication client

Purpose:
- Handles OAuth 2.0 client_credentials authentication for the eBay API
- Retrieves and caches access tokens for authenticated API requests
- Automatically refreshes tokens when expired

Notes:
- Designed for read-only eBay Browse API access
"""


import base64
import os
import time
import requests
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"

@dataclass
class EbayOAuthToken:
    access_token: str
    expires_at: float


class EbayAuthError(Exception):
    """Raised when eBay OAuth authentication fails"""


class EbayAuthClient:
    def __init__(
            self,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None
    ):
        self.client_id = client_id or os.getenv("EBAY_CLIENT_ID").strip()
        self.client_secret = client_secret or os.getenv("EBAY_CLIENT_SECRET").strip()

        if not self.client_id or not self.client_secret:
            raise EbayAuthError(
                "EBAY_CLIENT_ID and EBAY_CLIENT_SECRET must be set"
            )
        
        self._token: Optional[EbayOAuthToken] = None

    def get_access_token(self) -> str:
        """
        Returns a valid OAuth access token.
        Refreshes the token if expired or missing.
        """
        if self._token and not self._is_expired(self._token):
            return self._token.access_token
        
        self._token = self._fetch_new_token()
        return self._token.access_token
    
    def _is_expired(self, token:EbayOAuthToken) -> bool:
        # Refresh slightly early to avoid race conditions
        return time.time() >= token.expires_at - 60
    
    def _fetch_new_token(self) -> EbayOAuthToken:
        auth_header = self._build_auth_header()

        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = (
            f"grant_type=client_credentials"
            f"&scope={EBAY_SCOPE}"
        )


        response = requests.post(
            EBAY_OAUTH_URL,
            headers=headers,
            data=data,
            timeout=30
        )

        if response.status_code != 200:
            raise EbayAuthError(
                f"Failed to obtain OAuth token "
                f"(status={response.status_code}, body={response.text})"
            )
        
        body = response.json()

        return EbayOAuthToken(
            access_token=body["access_token"],
            expires_at=time.time() + body["expires_in"]
        )
        
    def _build_auth_header(self) -> str:
        """
        Builds HTTP Basic Auth header required by eBay OAuth.
        """
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"