# crypto.py
"""
Secure encryption/decryption module for Python objects using Scrypt KDF and ChaCha20-Poly1305.

Features:
- Password-based encryption with strong key derivation
- Master-key pattern for performance and key rotation
- Authenticated encryption ensuring data integrity
- Comprehensive error handling without information leakage
- KDF benchmarking and parameter tuning utilities
"""

import os
import json
import base64
import time
from typing import Dict, Any, Optional
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.exceptions import InvalidTag

# Cryptographic parameters - TEST PERFORMANCE ON YOUR TARGET DEPLOYMENT HARDWARE
_KDF_N = 2**17  # CPU/memory cost parameter (131072 - strong, adjust as needed)
_KDF_R = 8
_KDF_P = 1
_SALT_LEN = 16
_NONCE_LEN = 12
_KEY_LEN = 32
_MIN_PASSWORD_LENGTH = 8
_VERSION = "1.0"

# -------------------------
# Low-level primitives
# -------------------------
def derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 32-byte cryptographic key from password and salt using Scrypt."""
    if not password or len(password) < _MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {_MIN_PASSWORD_LENGTH} characters")
    if not isinstance(salt, (bytes, bytearray)) or len(salt) != _SALT_LEN:
        raise ValueError(f"Salt must be {_SALT_LEN} bytes")
    kdf = Scrypt(salt=salt, length=_KEY_LEN, n=_KDF_N, r=_KDF_R, p=_KDF_P)
    return kdf.derive(password.encode("utf-8"))

# -------------------------
# Password-based encrypt/decrypt (per-object KDF)
# -------------------------
def encrypt_object(obj: Dict[str, Any], password: str) -> str:
    """
    Encrypt a JSON-serializable dictionary using password-based encryption.
    Returns a JSON string containing base64 salt, nonce, ciphertext and version.
    """
    if not isinstance(obj, dict):
        raise TypeError("Expected dictionary object")
    if not password:
        raise ValueError("Password cannot be empty")

    # Ensure object is JSON-serializable early
    try:
        plaintext = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except TypeError:
        raise TypeError("Object is not JSON-serializable")

    salt = os.urandom(_SALT_LEN)
    nonce = os.urandom(_NONCE_LEN)
    key = derive_key(password, salt)
    aead = ChaCha20Poly1305(key)

    try:
        ciphertext = aead.encrypt(nonce, plaintext, None)
    except Exception:
        raise ValueError("Encryption failed")

    payload = {
        "v": _VERSION,
        "salt": base64.b64encode(salt).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ct": base64.b64encode(ciphertext).decode()
    }
    return json.dumps(payload)

def decrypt_object(payload_json: str, password: str) -> Dict[str, Any]:
    """
    Decrypt a payload produced by encrypt_object and return the original dictionary.
    Raises ValueError on any failure (wrong password, corrupted data, invalid payload).
    """
    if not payload_json:
        raise ValueError("Payload cannot be empty")
    if not password:
        raise ValueError("Password cannot be empty")

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid payload format")

    required_fields = {"salt", "nonce", "ct"}
    if not all(field in payload for field in required_fields):
        raise ValueError("Invalid payload structure: missing required fields")

    try:
        salt = base64.b64decode(payload["salt"])
        nonce = base64.b64decode(payload["nonce"])
        ciphertext = base64.b64decode(payload["ct"])
    except Exception:
        raise ValueError("Invalid base64 encoding in payload")

    if len(salt) != _SALT_LEN or len(nonce) != _NONCE_LEN:
        raise ValueError("Invalid cryptographic parameter lengths")

    # Version check
    if payload.get("v") != _VERSION:
        raise ValueError("Unsupported payload version")

    try:
        key = derive_key(password, salt)
        aead = ChaCha20Poly1305(key)
        plaintext = aead.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext.decode("utf-8"))
    except InvalidTag:
        raise ValueError("Decryption failed - incorrect password or corrupted data")
    except (ValueError, json.JSONDecodeError):
        raise ValueError("Decryption failed - corrupted data or invalid payload")
    except Exception:
        raise ValueError("Decryption failed due to unexpected error")

# -------------------------
# Master-key wrapping (recommended for performance & rotation)
# -------------------------
def generate_master_key() -> bytes:
    """Generate a random 32-byte master key to use for fast symmetric encryption."""
    return os.urandom(_KEY_LEN)

def wrap_master_key(master_key: bytes, password: str) -> str:
    """Wrap (encrypt) the master key with a password-derived key."""
    if not isinstance(master_key, (bytes, bytearray)) or len(master_key) != _KEY_LEN:
        raise ValueError("master_key must be 32 bytes")
    if not password:
        raise ValueError("Password cannot be empty")

    salt = os.urandom(_SALT_LEN)
    nonce = os.urandom(_NONCE_LEN)
    key = derive_key(password, salt)
    aead = ChaCha20Poly1305(key)
    ct = aead.encrypt(nonce, master_key, None)
    payload = {
        "v": "wrap1",
        "salt": base64.b64encode(salt).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ct": base64.b64encode(ct).decode()
    }
    return json.dumps(payload)

def unwrap_master_key(wrapped_payload_json: str, password: str) -> bytes:
    """Unwrap master key previously produced by wrap_master_key."""
    try:
        payload = json.loads(wrapped_payload_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid wrapped payload format")
    
    if payload.get("v") != "wrap1":
        raise ValueError("Unsupported wrapped-key version")
    
    try:
        salt = base64.b64decode(payload["salt"])
        nonce = base64.b64decode(payload["nonce"])
        ct = base64.b64decode(payload["ct"])
    except Exception:
        raise ValueError("Invalid wrapped payload encoding")
    
    if len(salt) != _SALT_LEN or len(nonce) != _NONCE_LEN:
        raise ValueError("Invalid wrapped-key parameter lengths")
    
    try:
        key = derive_key(password, salt)
        aead = ChaCha20Poly1305(key)
        master_key = aead.decrypt(nonce, ct, None)
        if len(master_key) != _KEY_LEN:
            raise ValueError("Invalid master key length after unwrap")
        return master_key
    except InvalidTag:
        raise ValueError("Unwrap failed - incorrect password or corrupted wrapped key")
    except Exception:
        raise ValueError("Unwrap failed due to unexpected error")

def encrypt_with_master_key(obj: Dict[str, Any], master_key: bytes) -> str:
    """Encrypt object using a raw master key (fast; no KDF per object)."""
    if not isinstance(obj, dict):
        raise TypeError("Expected dictionary object")
    if not isinstance(master_key, (bytes, bytearray)) or len(master_key) != _KEY_LEN:
        raise ValueError("master_key must be 32 bytes")

    try:
        plaintext = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except TypeError:
        raise TypeError("Object is not JSON-serializable")

    nonce = os.urandom(_NONCE_LEN)
    aead = ChaCha20Poly1305(master_key)
    try:
        ct = aead.encrypt(nonce, plaintext, None)
    except Exception:
        raise ValueError("Encryption with master key failed")
    
    payload = {
        "v": _VERSION,
        "nonce": base64.b64encode(nonce).decode(),
        "ct": base64.b64encode(ct).decode()
    }
    return json.dumps(payload)

def decrypt_with_master_key(payload_json: str, master_key: bytes) -> Dict[str, Any]:
    """Decrypt a payload produced by encrypt_with_master_key using the master key."""
    if not payload_json:
        raise ValueError("Payload cannot be empty")
    if not isinstance(master_key, (bytes, bytearray)) or len(master_key) != _KEY_LEN:
        raise ValueError("master_key must be 32 bytes")

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid payload format")

    if "nonce" not in payload or "ct" not in payload:
        raise ValueError("Invalid payload structure for master-key decryption")

    try:
        nonce = base64.b64decode(payload["nonce"])
        ct = base64.b64decode(payload["ct"])
    except Exception:
        raise ValueError("Invalid base64 encoding in payload")

    if len(nonce) != _NONCE_LEN:
        raise ValueError("Invalid nonce length")

    try:
        aead = ChaCha20Poly1305(master_key)
        pt = aead.decrypt(nonce, ct, None)
        return json.loads(pt.decode("utf-8"))
    except InvalidTag:
        raise ValueError("Decryption failed - incorrect master key or corrupted data")
    except (ValueError, json.JSONDecodeError):
        raise ValueError("Decryption failed - corrupted data or invalid payload")
    except Exception:
        raise ValueError("Decryption failed due to unexpected error")

# -------------------------
# Utility: tune / benchmark
# -------------------------
def adjust_security_parameters(n: Optional[int] = None, r: Optional[int] = None,
                               p: Optional[int] = None, min_password_length: Optional[int] = None) -> None:
    """
    Adjust module-level KDF and password-policy parameters.
    Validates basic constraints: n must be a power of two and >= 2**12.
    """
    global _KDF_N, _KDF_R, _KDF_P, _MIN_PASSWORD_LENGTH
    if n is not None:
        if not isinstance(n, int) or n < 2**12 or (n & (n - 1)) != 0:
            raise ValueError("n must be a power-of-two integer >= 2**12")
        _KDF_N = n
    if r is not None:
        if not isinstance(r, int) or r <= 0:
            raise ValueError("r must be a positive integer")
        _KDF_R = r
    if p is not None:
        if not isinstance(p, int) or p <= 0:
            raise ValueError("p must be a positive integer")
        _KDF_P = p
    if min_password_length is not None:
        if not isinstance(min_password_length, int) or min_password_length < 1:
            raise ValueError("min_password_length must be a positive integer")
        _MIN_PASSWORD_LENGTH = min_password_length

def benchmark_kdf(password: str = "benchmark", iterations: int = 1) -> float:
    """
    Run derive_key once (or multiple iterations) and return average time in seconds.
    Use this on each target device to pick an appropriate _KDF_N.
    """
    salt = os.urandom(_SALT_LEN)
    start = time.time()
    for _ in range(max(1, iterations)):
        derive_key(password, salt)
    end = time.time()
    return (end - start) / max(1, iterations)

# -------------------------
# Simple self-test when run directly
# -------------------------
if __name__ == "__main__":
    sample = {
        "id": "c1",
        "name": "Alice Example",
        "phones": ["+919900112233"],
        "notes": "This is a secret note."
    }
    password = "strong_password_123"

    print(">> Benchmarking KDF (a single derive) - tune _KDF_N by testing on your device")
    t = benchmark_kdf(password, iterations=1)
    print(f"   derive_key time: {t:.3f}s (adjust _KDF_N if this is too slow)")

    # Per-object (password-derived key) flow
    enc = encrypt_object(sample, password)
    dec = decrypt_object(enc, password)
    assert dec == sample
    print("✓ password-based encrypt/decrypt roundtrip")

    try:
        decrypt_object(enc, "wrong-password")
        print("ERROR: wrong password succeeded (should not happen)")
    except ValueError:
        print("✓ wrong password correctly rejected (password-based)")

    # Master-key flow
    mk = generate_master_key()
    wrapped = wrap_master_key(mk, password)
    unwrapped = unwrap_master_key(wrapped, password)
    assert mk == unwrapped
    print("✓ master-key wrap/unwrap roundtrip")

    enc2 = encrypt_with_master_key(sample, mk)
    dec2 = decrypt_with_master_key(enc2, mk)
    assert dec2 == sample
    print("✓ master-key encrypt/decrypt roundtrip")

    try:
        decrypt_with_master_key(enc2, b"\x00" * 32)
        print("ERROR: wrong master-key succeeded (should not happen)")
    except ValueError:
        print("✓ wrong master-key correctly rejected")

    print("\nAll local tests passed.")