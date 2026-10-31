# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-account>=0.13,<0.14", "requests>=2.32,<3", "pytest>=8,<9"]
# ///
"""Offline checks: header shape, signature recovers to the signer, create->derive fallback, env writing."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from eth_account import Account  # noqa: E402
from eth_account.messages import encode_typed_data  # noqa: E402

import derive_clob_credentials as d  # noqa: E402


def test_signature_recovers_to_signer():
    acct = Account.create()
    h = d.l1_headers(acct, nonce=0, timestamp=1793431000)
    assert h["POLY_ADDRESS"] == acct.address and h["POLY_TIMESTAMP"] == "1793431000" and h["POLY_NONCE"] == "0"
    msg = encode_typed_data(domain_data={"name": "ClobAuthDomain", "version": "1", "chainId": 137},
                            message_types={"ClobAuth": [{"name": "address", "type": "address"}, {"name": "timestamp", "type": "string"},
                                                        {"name": "nonce", "type": "uint256"}, {"name": "message", "type": "string"}]},
                            message_data={"address": acct.address, "timestamp": "1793431000", "nonce": 0, "message": d.ATTESTATION})
    assert Account.recover_message(msg, signature=h["POLY_SIGNATURE"]) == acct.address


class Resp:
    def __init__(self, status, body):
        self.status_code, self._body, self.ok, self.text = status, body, 200 <= status < 300, str(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = responses, []

    def request(self, method, url, headers, timeout):
        self.calls.append((method, url.rsplit("/", 1)[-1]))
        return self.responses.pop(0)


def test_create_then_derive_fallback():
    creds = {"apiKey": "k", "secret": "s", "passphrase": "p"}
    s = FakeSession([Resp(400, {"error": "exists"}), Resp(200, creds)])
    assert d.get_credentials(Account.create(), session=s) == creds
    assert s.calls == [("POST", "api-key"), ("GET", "derive-api-key")]


def test_env_file_updated_in_place(tmp_path):
    f = tmp_path / ".env.local"
    f.write_text("APP_ENV=local\nPOLY_API_KEY=\nPOLY_API_SECRET=\nPOLY_API_PASSPHRASE=\nLOG_LEVEL=info\n")
    d.write_env(f, {"apiKey": "k1", "secret": "s1", "passphrase": "p1"})
    assert f.read_text() == "APP_ENV=local\nPOLY_API_KEY=k1\nPOLY_API_SECRET=s1\nPOLY_API_PASSPHRASE=p1\nLOG_LEVEL=info\n"
    assert f.stat().st_mode & 0o777 == 0o600
