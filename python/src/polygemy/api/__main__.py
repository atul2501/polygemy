"""polygemy API.

  python -m polygemy.api serve                       # API_HOST (default 127.0.0.1), API_PORT (8000)
  python -m polygemy.api create-user NAME --role admin|viewer   # password from POLYGEMY_NEW_PASSWORD or prompt
"""

import argparse
import getpass
import os
import sys

import psycopg


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="polygemy.api")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    cu = sub.add_parser("create-user")
    cu.add_argument("username")
    cu.add_argument("--role", choices=["viewer", "admin"], required=True)
    a = p.parse_args(argv)
    if a.cmd == "serve":
        import uvicorn

        from .app import create_app

        uvicorn.run(create_app(), host=os.environ.get("API_HOST", "127.0.0.1"), port=int(os.environ.get("API_PORT", "8000")), log_level="info")
    else:
        from . import auth

        pw = os.environ.get("POLYGEMY_NEW_PASSWORD") or getpass.getpass(f"password for {a.username}: ")
        with psycopg.connect(os.environ["DATABASE_URL"]) as c:
            auth.create_user(c, a.username, pw, a.role)
        print(f"user {a.username} ({a.role}) saved")
    return 0


if __name__ == "__main__":
    sys.exit(main())
