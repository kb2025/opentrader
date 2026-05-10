"""
Shared Fernet encryption helpers.
Derives a 32-byte key from SECRET_KEY via SHA-256 so any string secret
can be used directly without manual base64 padding.
"""
import base64
import hashlib
import os


def _fernet(secret_key: str | None = None):
    from cryptography.fernet import Fernet
    key = secret_key or os.environ.get("SECRET_KEY", "")
    raw = hashlib.sha256(key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(raw))


def encrypt_secret(value: str, secret_key: str | None = None) -> str:
    return _fernet(secret_key).encrypt(value.encode()).decode()


def decrypt_secret(token: str, secret_key: str | None = None) -> str:
    return _fernet(secret_key).decrypt(token.encode()).decode()
