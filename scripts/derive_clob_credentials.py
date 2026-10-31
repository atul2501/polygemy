# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-account>=0.13,<0.14", "requests>=2.32,<3"]
# ///
"""Get Polymarket CLOB API credentials (key, secret, passphrase).

Official flow (docs.polymarket.com/getting-started/api.md#authentication): sign the EIP-712
`ClobAuth` message with a wallet key (L1), then POST /auth/api-key (create) or
GET /auth/derive-api-key (derive existing).

Default: generates a fresh, unfunded wallet used only for reading market data (Chainlink via
PolyBolt). Your real Polymarket account key is not needed and never touches this machine's disk.

  uv run scripts/derive_clob_credentials.py --env-file .env.local
      generate a data-only wallet, write the three values into .env.local (nothing printed)
  POLYMARKET_SIGNER_KEY=0x... uv run scripts/derive_clob_credentials.py --print
      derive for an existing key, print the values
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path

import requests
from eth_account import Account

CLOB = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon
ATTESTATION = "This message attests that I control the given wallet"


def l1_headers(account, nonce: int = 0, timestamp: int | None = None) -> dict[str, str]:
    ts = str(timestamp if timestamp is not None else int(time.time()))
    typed = {
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": CHAIN_ID},
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "ClobAuth": [
                {"name": "address", "type": "address"},
                {"name": "timestamp", "type": "string"},
                {"name": "nonce", "type": "uint256"},
                {"name": "message", "type": "string"},
            ],
        },
        "primaryType": "ClobAuth",
        "message": {"address": account.address, "timestamp": ts, "nonce": nonce, "message": ATTESTATION},
    }
    sig = Account.sign_typed_data(account.key, full_message=typed).signature.hex()
    return {
        "POLY_ADDRESS": account.address,
        "POLY_SIGNATURE": sig if sig.startswith("0x") else "0x" + sig,
        "POLY_TIMESTAMP": ts,
        "POLY_NONCE": str(nonce),
    }


def get_credentials(account, base: str = CLOB, session=requests) -> dict[str, str]:
    """Create credentials; if they already exist for this address/nonce, derive them."""
    errors = []
    for method, path in (("POST", "/auth/api-key"), ("GET", "/auth/derive-api-key")):
        r = session.request(method, base + path, headers=l1_headers(account), timeout=15)
        if r.ok:
            body = r.json()
            if all(body.get(k) for k in ("apiKey", "secret", "passphrase")):
                return {"apiKey": body["apiKey"], "secret": body["secret"], "passphrase": body["passphrase"]}
        errors.append(f"{method} {path}: HTTP {r.status_code} {r.text[:200]}")
    raise RuntimeError("could not obtain CLOB credentials:\n  " + "\n  ".join(errors))


def write_env(path: Path, creds: dict[str, str]) -> None:
    """Set POLY_API_KEY/SECRET/PASSPHRASE in an env file in place; keeps every other line."""
    values = {"POLY_API_KEY": creds["apiKey"], "POLY_API_SECRET": creds["secret"], "POLY_API_PASSPHRASE": creds["passphrase"]}
    text = path.read_text() if path.exists() else ""
    for k, v in values.items():
        line = f"{k}={v}"
        text, n = re.subn(rf"^{k}=.*$", lambda _: line, text, flags=re.M)
        if n == 0:
            text += ("" if text.endswith("\n") or not text else "\n") + line + "\n"
    path.write_text(text)
    path.chmod(0o600)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env-file", type=Path, help="write the credentials into this env file (chmod 600)")
    p.add_argument("--print", action="store_true", help="print the credentials to the terminal")
    args = p.parse_args()
    if not args.env_file and not args.print:
        p.error("choose --env-file FILE and/or --print")

    key = os.environ.get("POLYMARKET_SIGNER_KEY")
    account = Account.from_key(key) if key else Account.create()
    origin = "existing key from POLYMARKET_SIGNER_KEY" if key else "new data-only wallet (unfunded, key not stored)"
    print(f"signer address: {account.address} ({origin})")

    creds = get_credentials(account)
    if args.env_file:
        write_env(args.env_file, creds)
        print(f"wrote POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE to {args.env_file} (chmod 600)")
    if args.print:
        print(f"POLY_API_KEY={creds['apiKey']}\nPOLY_API_SECRET={creds['secret']}\nPOLY_API_PASSPHRASE={creds['passphrase']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
