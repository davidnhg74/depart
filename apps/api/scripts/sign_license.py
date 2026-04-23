"""Sign a depart license JWT (dev helper).

Reads the private key from ~/.depart-keys/license_private_dev.pem by
default and emits an RS256 JWT with the claims the verifier expects.
Production licenses are signed by a separate, out-of-band tool against
the production keypair — this script exists only for local testing
and for customer trials.

Usage:

    python scripts/sign_license.py \
        --subject ops@acme.com \
        --project acme-2026q2 \
        --tier pro \
        --features ai_conversion,runbook_pdf,webhooks \
        --days 90

Writes the JWT to stdout. Pipe into the /api/v1/license endpoint (once
that ships) or paste into the /settings/instance UI.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from jose import jwt


DEFAULT_KEY_PATH = Path.home() / ".depart-keys" / "license_private_dev.pem"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--subject", required=True, help="Email or identifier of the license holder")
    p.add_argument("--project", required=True, help="Project slug, e.g. acme-migration-2026q2")
    p.add_argument("--tier", default="pro", choices=["pro", "enterprise"])
    p.add_argument(
        "--features",
        default="ai_conversion,runbook_pdf",
        help="Comma-separated feature flags",
    )
    p.add_argument("--days", type=int, default=90, help="Validity in days from now")
    p.add_argument(
        "--key",
        type=Path,
        default=DEFAULT_KEY_PATH,
        help=f"Private key PEM (default: {DEFAULT_KEY_PATH})",
    )
    args = p.parse_args()

    try:
        private_pem = args.key.read_text()
    except FileNotFoundError:
        print(
            f"private key not found at {args.key}. generate one with:\n"
            "  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 "
            "-out ~/.depart-keys/license_private_dev.pem",
            file=sys.stderr,
        )
        return 1

    now = int(time.time())
    claims = {
        "sub": args.subject,
        "project": args.project,
        "tier": args.tier,
        "features": [f.strip() for f in args.features.split(",") if f.strip()],
        "iat": now,
        "exp": now + args.days * 86400,
    }

    token = jwt.encode(claims, private_pem, algorithm="RS256")
    sys.stdout.write(token + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
