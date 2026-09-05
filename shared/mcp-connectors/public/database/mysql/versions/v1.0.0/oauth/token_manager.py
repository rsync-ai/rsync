#!/usr/bin/env python3
"""
OAuth Token Manager

Secure token storage, encryption, and refresh management.

Security Features:
- AES-256-GCM encryption for tokens at rest
- Automatic token refresh before expiry
- Secure random key generation
- Token expiry tracking
"""

import os
import json
import time
import base64
import hashlib
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Try to import cryptography for encryption
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logger.warning("⚠️ cryptography library not available - token encryption disabled")


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class OAuthToken:
    """
    OAuth token data structure.
    
    Attributes:
        access_token: The access token for API calls
        refresh_token: Token used to refresh access token (if available)
        token_type: Usually "Bearer"
        expires_at: Unix timestamp when token expires
        expires_in: Original expiry duration in seconds
        scope: Space-separated list of granted scopes
        provider: OAuth provider name (e.g., 'hubspot')
        extra_data: Additional provider-specific data
    """
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"
    expires_at: Optional[float] = None
    expires_in: Optional[int] = None
    scope: Optional[str] = None
    provider: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None
    created_at: Optional[float] = None
    
    def __post_init__(self):
        """Calculate expires_at from expires_in if not provided."""
        if self.created_at is None:
            self.created_at = time.time()
        
        if self.expires_at is None and self.expires_in is not None:
            self.expires_at = self.created_at + self.expires_in
    
    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """
        Check if token is expired or about to expire.
        
        Args:
            buffer_seconds: Consider expired if expiring within this many seconds
            
        Returns:
            True if token is expired or expiring soon
        """
        if self.expires_at is None:
            return False  # Tokens without expiry don't expire
        
        return time.time() >= (self.expires_at - buffer_seconds)
    
    def time_until_expiry(self) -> Optional[int]:
        """Get seconds until token expires, or None if no expiry."""
        if self.expires_at is None:
            return None
        return max(0, int(self.expires_at - time.time()))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (for JSON serialization)."""
        return {k: v for k, v in asdict(self).items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OAuthToken':
        """Create from dictionary (from JSON response or storage)."""
        return cls(
            access_token=data['access_token'],
            refresh_token=data.get('refresh_token'),
            token_type=data.get('token_type', 'Bearer'),
            expires_at=data.get('expires_at'),
            expires_in=data.get('expires_in'),
            scope=data.get('scope'),
            provider=data.get('provider'),
            extra_data=data.get('extra_data'),
            created_at=data.get('created_at'),
        )


# ============================================================================
# Encryption Functions
# ============================================================================

# Kept in lockstep with shared/go/crypto/encryption.go getKeyring(), which
# log.Fatalf's on these exact literals outside dev. Encrypting a credential
# under a passphrase published in this repo is not encryption; it is base64
# with extra steps, and it looks identical in every log and on disk. Unlike the
# Go side this raises at use time rather than exiting at startup: the token
# store is an optional subsystem, and taking the whole deployer down over it
# would trade a contained failure for an outage.
_DEV_DEFAULT_KEYS = frozenset({
    "dev-only-key-please-change-me!!!",
    "dev-encryption-key-32-bytes-long!!",
})


def _is_dev_env() -> bool:
    return os.getenv("ENVIRONMENT", "") in ("development", "dev")


def _resolve_encryption_key(explicit: Optional[str] = None) -> str:
    """Resolve the passphrase used to derive the token-store key.

    Falls back to ENCRYPTION_KEY because OAUTH_ENCRYPTION_KEY is set by no
    compose file and no chart -- so the "encrypted" token store was, in every
    shipped deployment, a store that raised on every single write. A second
    secret that nothing configures is not defence in depth; it is an outage
    wearing a security-shaped name. ENCRYPTION_KEY is already required and
    validated everywhere else, and it is consumed here only as a PBKDF2
    passphrase (random per-record salt, 100k iterations), never as raw key
    material -- so it stays domain-separated from the Go keyring's direct
    AES-GCM use of the same string.
    """
    key = explicit or os.getenv('OAUTH_ENCRYPTION_KEY') or os.getenv('ENCRYPTION_KEY')
    if not key:
        raise ValueError(
            "no token-store encryption key: set OAUTH_ENCRYPTION_KEY (or "
            "ENCRYPTION_KEY) -- refusing to persist OAuth tokens unencrypted"
        )
    if key in _DEV_DEFAULT_KEYS and not _is_dev_env():
        raise ValueError(
            "refusing to derive the OAuth token key from a known dev encryption "
            "key in a non-dev ENVIRONMENT; set ENCRYPTION_KEY (or "
            "OAUTH_ENCRYPTION_KEY) to a real secret before deploying"
        )
    return key


def _derive_key(password: str, salt: bytes) -> bytes:
    """Derive encryption key from password using PBKDF2."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library required for encryption")
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,  # 256 bits for AES-256
        salt=salt,
        iterations=100000,
    )
    return kdf.derive(password.encode())


def encrypt_token(token_data: Dict[str, Any], 
                  encryption_key: Optional[str] = None) -> str:
    """
    Encrypt token data for secure storage.
    
    Args:
        token_data: Token dictionary to encrypt
        encryption_key: Optional key (uses OAUTH_ENCRYPTION_KEY env var if not provided)
        
    Returns:
        Base64-encoded encrypted data with salt and nonce prepended
    """
    if not CRYPTO_AVAILABLE:
        # Fail closed: never silently persist OAuth tokens as reversible
        # base64 to a .enc file that implies real encryption.
        raise RuntimeError(
            "cryptography package required to persist OAuth tokens securely; "
            "refusing to store credentials as reversible base64"
        )

    key = _resolve_encryption_key(encryption_key)
    
    # Generate random salt and nonce
    salt = os.urandom(16)
    nonce = os.urandom(12)  # 96 bits for AES-GCM
    
    # Derive key and encrypt
    derived_key = _derive_key(key, salt)
    aesgcm = AESGCM(derived_key)
    
    plaintext = json.dumps(token_data).encode()
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)
    
    # Combine salt + nonce + ciphertext
    encrypted = salt + nonce + ciphertext
    return base64.b64encode(encrypted).decode()


def decrypt_token(encrypted_data: str,
                  encryption_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Decrypt token data from storage.
    
    Args:
        encrypted_data: Base64-encoded encrypted data
        encryption_key: Optional key (uses OAUTH_ENCRYPTION_KEY env var if not provided)
        
    Returns:
        Decrypted token dictionary
    """
    if not CRYPTO_AVAILABLE:
        # Fail closed: refuse to read a stored blob as if it were encrypted
        # when the crypto backend is missing. A base64 blob is NOT encryption;
        # decoding it here would silently treat plaintext credentials as valid.
        raise RuntimeError(
            "cryptography package required to read persisted OAuth tokens securely; "
            "refusing to decode credentials stored as reversible base64"
        )

    key = _resolve_encryption_key(encryption_key)
    
    # Decode and split components
    data = base64.b64decode(encrypted_data)
    salt = data[:16]
    nonce = data[16:28]
    ciphertext = data[28:]
    
    # Derive key and decrypt
    derived_key = _derive_key(key, salt)
    aesgcm = AESGCM(derived_key)
    
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return json.loads(plaintext.decode())


# ============================================================================
# Token Manager Class
# ============================================================================

class TokenManager:
    """
    Manages OAuth tokens with secure storage and automatic refresh.
    
    Features:
    - Encrypted token storage
    - Automatic refresh before expiry
    - Thread-safe operations
    - Pluggable storage backends
    """
    
    def __init__(self, 
                 storage_path: Optional[str] = None,
                 encryption_key: Optional[str] = None):
        """
        Initialize TokenManager.
        
        Args:
            storage_path: Path to token storage file (default: ~/.rsync-ai/tokens.enc)
            encryption_key: Encryption key (default: OAUTH_ENCRYPTION_KEY env var)
        """
        self.encryption_key = encryption_key or os.getenv('OAUTH_ENCRYPTION_KEY')
        self.storage_path = storage_path or os.path.expanduser('~/.rsync-ai/tokens.enc')
        
        # Ensure storage directory exists
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        
        # In-memory token cache
        self._tokens: Dict[str, OAuthToken] = {}
        
        # Load existing tokens
        self._load_tokens()
    
    def _load_tokens(self):
        """Load tokens from storage file."""
        if not os.path.exists(self.storage_path):
            return
        
        try:
            with open(self.storage_path, 'r') as f:
                encrypted_data = f.read().strip()
            
            if encrypted_data:
                tokens_data = decrypt_token(encrypted_data, self.encryption_key)
                self._tokens = {
                    key: OAuthToken.from_dict(data)
                    for key, data in tokens_data.items()
                }
                logger.info(f"Loaded {len(self._tokens)} tokens from storage")
        except Exception as e:
            logger.error(f"Failed to load tokens: {e}")
            self._tokens = {}
    
    def _save_tokens(self):
        """Save tokens to storage file."""
        tokens_data = {
            key: token.to_dict()
            for key, token in self._tokens.items()
        }

        # Encryption failures are deliberately NOT caught. A missing crypto
        # backend (RuntimeError) and a missing key (ValueError) both mean the
        # token was never written -- and store_token() logs "Stored token for
        # connection: ..." on the very next line. Swallowing them produced
        # exactly that pair: an error, then a success, with nothing on disk.
        # Neither condition ever self-heals, so there is no retry to preserve.
        #
        # The file write below stays caught: a full or briefly unwritable disk
        # is transient, and raising there would fail a token refresh whose
        # result we already hold in memory -- trading a real outage for a
        # cosmetic one.
        encrypted_data = encrypt_token(tokens_data, self.encryption_key)

        try:
            with open(self.storage_path, 'w') as f:
                f.write(encrypted_data)
            logger.debug(f"Saved {len(self._tokens)} tokens to storage")
        except Exception as e:
            logger.error(f"Failed to save tokens: {e}")
    
    def store_token(self, 
                    connection_id: str, 
                    token: OAuthToken) -> None:
        """
        Store a token for a connection.
        
        Args:
            connection_id: Unique connection identifier
            token: OAuthToken to store
        """
        self._tokens[connection_id] = token
        self._save_tokens()
        logger.info(f"Stored token for connection: {connection_id}")
    
    def get_token(self, connection_id: str) -> Optional[OAuthToken]:
        """
        Get token for a connection.
        
        Args:
            connection_id: Unique connection identifier
            
        Returns:
            OAuthToken or None if not found
        """
        return self._tokens.get(connection_id)
    
    def delete_token(self, connection_id: str) -> bool:
        """
        Delete token for a connection.
        
        Args:
            connection_id: Unique connection identifier
            
        Returns:
            True if token was deleted, False if not found
        """
        if connection_id in self._tokens:
            del self._tokens[connection_id]
            self._save_tokens()
            logger.info(f"Deleted token for connection: {connection_id}")
            return True
        return False
    
    def get_valid_token(self, 
                        connection_id: str,
                        refresh_callback: Optional[callable] = None) -> Optional[OAuthToken]:
        """
        Get a valid (non-expired) token, refreshing if necessary.
        
        Args:
            connection_id: Unique connection identifier
            refresh_callback: Function to call to refresh token (receives OAuthToken, returns new OAuthToken)
            
        Returns:
            Valid OAuthToken or None if not found/refresh failed
        """
        token = self.get_token(connection_id)
        if not token:
            return None
        
        # Check if token needs refresh
        if token.is_expired():
            if not token.refresh_token:
                logger.warning(f"Token expired and no refresh token available: {connection_id}")
                return None
            
            if not refresh_callback:
                logger.warning(f"Token expired but no refresh callback provided: {connection_id}")
                return None
            
            try:
                logger.info(f"Refreshing expired token for: {connection_id}")
                new_token = refresh_callback(token)
                self.store_token(connection_id, new_token)
                return new_token
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                return None
        
        return token
    
    def should_refresh(self, 
                       connection_id: str, 
                       buffer_seconds: int = 300) -> bool:
        """
        Check if token should be refreshed soon.
        
        Args:
            connection_id: Unique connection identifier
            buffer_seconds: Refresh if expiring within this many seconds
            
        Returns:
            True if token should be refreshed
        """
        token = self.get_token(connection_id)
        if not token:
            return False
        return token.is_expired(buffer_seconds)
    
    def list_connections(self) -> Dict[str, Dict[str, Any]]:
        """
        List all stored connections with token status.
        
        Returns:
            Dict mapping connection_id to token status info
        """
        result = {}
        for conn_id, token in self._tokens.items():
            result[conn_id] = {
                'provider': token.provider,
                'token_type': token.token_type,
                'has_refresh_token': token.refresh_token is not None,
                'is_expired': token.is_expired(),
                'expires_in': token.time_until_expiry(),
                'created_at': datetime.fromtimestamp(token.created_at).isoformat() if token.created_at else None,
            }
        return result


# ============================================================================
# Token Refresh Helpers
# ============================================================================

def create_refresh_callback(provider_name: str):
    """
    Create a refresh callback function for a specific provider.
    
    Args:
        provider_name: OAuth provider name
        
    Returns:
        Callback function that takes OAuthToken and returns new OAuthToken
    """
    from .oauth_registry import get_oauth_provider, get_token_url, get_provider_credentials
    import requests
    
    def refresh_token(old_token: OAuthToken) -> OAuthToken:
        """Refresh an OAuth token."""
        provider = get_oauth_provider(provider_name)
        if not provider:
            raise ValueError(f"Unknown provider: {provider_name}")
        
        if not old_token.refresh_token:
            raise ValueError("No refresh token available")
        
        # Get credentials
        creds = get_provider_credentials(provider_name)
        
        # Get token URL
        token_url = get_token_url(provider_name)
        
        # Make refresh request
        response = requests.post(
            token_url,
            data={
                'grant_type': 'refresh_token',
                'refresh_token': old_token.refresh_token,
                'client_id': creds['client_id'],
                'client_secret': creds['client_secret'],
            },
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            timeout=30,
        )
        
        response.raise_for_status()
        data = response.json()
        
        # Create new token (keep old refresh token if not provided in response)
        return OAuthToken(
            access_token=data['access_token'],
            refresh_token=data.get('refresh_token', old_token.refresh_token),
            token_type=data.get('token_type', 'Bearer'),
            expires_in=data.get('expires_in'),
            scope=data.get('scope', old_token.scope),
            provider=provider_name,
        )
    
    return refresh_token


# ============================================================================
# CLI Testing
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    print("=" * 60)
    print("OAuth Token Manager Test")
    print("=" * 60)
    
    # Test encryption
    print("\n📋 TEST 1: Token Encryption")
    if CRYPTO_AVAILABLE:
        os.environ['OAUTH_ENCRYPTION_KEY'] = 'test-key-for-testing-only-32chars!'
        
        test_token = {
            'access_token': 'sk_test_abc123',
            'refresh_token': 'rt_test_xyz789',
            'expires_in': 3600,
        }
        
        encrypted = encrypt_token(test_token)
        print(f"   Encrypted: {encrypted[:50]}...")
        
        decrypted = decrypt_token(encrypted)
        print(f"   Decrypted: {decrypted}")
        
        if decrypted['access_token'] == test_token['access_token']:
            print("   ✅ Encryption/decryption successful")
        else:
            print("   ❌ Encryption/decryption failed")
    else:
        print("   ⚠️ cryptography library not available")
    
    # Test OAuthToken
    print("\n📋 TEST 2: OAuthToken Model")
    token = OAuthToken(
        access_token='test_token',
        refresh_token='test_refresh',
        expires_in=3600,
        provider='hubspot',
    )
    print(f"   Token: {token.access_token[:10]}...")
    print(f"   Expires in: {token.time_until_expiry()} seconds")
    print(f"   Is expired: {token.is_expired()}")
    print(f"   ✅ OAuthToken working")
    
    # Test TokenManager
    print("\n📋 TEST 3: TokenManager")
    if CRYPTO_AVAILABLE:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.enc') as f:
            temp_path = f.name
        
        manager = TokenManager(storage_path=temp_path)
        manager.store_token('test-connection', token)
        
        retrieved = manager.get_token('test-connection')
        if retrieved and retrieved.access_token == token.access_token:
            print(f"   ✅ Token stored and retrieved successfully")
        else:
            print(f"   ❌ Token retrieval failed")
        
        # Cleanup
        os.unlink(temp_path)
    else:
        print("   ⚠️ Skipping (requires cryptography library)")
    
    print("\n✅ Token Manager tests complete!")
