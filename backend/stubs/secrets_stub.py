import os
from cryptography.fernet import Fernet

def get_fernet_key() -> bytes:
    # dev default: ephemeral key via env var or generated once and saved to file
    key = os.getenv("AARIA_FERNET_KEY")
    if key:
        return key.encode()
    # look for local .fernet_key
    kpath = os.path.join(os.path.dirname(__file__), ".fernet_key")
    if os.path.exists(kpath):
        return open(kpath, "rb").read().strip()
    key = Fernet.generate_key()
    with open(kpath, "wb") as f:
        f.write(key)
    return key

def get_fernet():
    return Fernet(get_fernet_key())