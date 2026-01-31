# routes/AngelOne/angel_login.py
import json
import socket
import uuid
import requests
import pyotp
from datetime import datetime
from config import (
    ANGEL_API_KEY,
    ANGEL_CLIENT_ID,
    ANGEL_CLIENT_PIN,
    ANGEL_TOTP_KEY,
)

LOGIN_URL = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"

def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def get_mac_address() -> str:
    mac = uuid.getnode()
    return ":".join(f"{(mac >> ele) & 0xff:02X}" for ele in range(40, -8, -8))

def get_totp(totp_key: str) -> str:
    return pyotp.TOTP(totp_key).now()

def save_tokens(tokens: dict, filename: str = "tokens.json"):
    payload = {
        "saved_at": datetime.now().isoformat(),
        **tokens
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"\n✅ Tokens saved to {filename}")

def login_and_get_token() -> dict:
    totp = get_totp(ANGEL_TOTP_KEY)
    print("current_otp :", totp)

    client_public_ip = "1.1.1.1"  # optional
    client_local_ip = get_local_ip()
    mac_addr = get_mac_address()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": client_local_ip,
        "X-ClientPublicIP": client_public_ip,
        "X-MACAddress": mac_addr,
        "X-PrivateKey": ANGEL_API_KEY,
    }

    payload = {
        "clientcode": ANGEL_CLIENT_ID,
        "password": ANGEL_CLIENT_PIN,
        "totp": totp,
        "state": "python-login",
    }

    resp = requests.post(LOGIN_URL, headers=headers, json=payload, timeout=20)
    resp.raise_for_status()

    data = resp.json()
    print("Raw response:", json.dumps(data, indent=2))

    if not data.get("data"):
        raise RuntimeError(f"Login failed: {data}")

    tokens = {
        "jwtToken": data["data"].get("jwtToken"),
        "refreshToken": data["data"].get("refreshToken"),
        "feedToken": data["data"].get("feedToken"),
        "state": data["data"].get("state"),
    }

    print("\n=== Tokens ===")
    for k, v in tokens.items():
        print(f"{k}: {v}")

    # ✅ SAVE HERE
    save_tokens(tokens, "tokens.json")
    return tokens

if __name__ == "__main__":
    login_and_get_token()
