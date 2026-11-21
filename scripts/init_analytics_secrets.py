"""Generate only missing analytics secrets; existing platform credentials are untouched."""

from pathlib import Path
import argparse
import secrets

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".auth/fnb",
    )
    parser.add_argument(
        "--compose-secrets",
        action="store_true",
        help="Read-only files shared across container UIDs; keep the host directory private",
    )
    args = parser.parse_args()
    target = args.directory
    target.mkdir(parents=True, exist_ok=True)
    for name in [
        "admin_password",
        "superset_metadata_password",
        "superset_secret_key",
        "superset_admin_password",
    ]:
        path = target / name
        try:
            with path.open("x", encoding="utf-8") as stream:
                stream.write(secrets.token_hex(32))
            path.chmod(0o444 if args.compose_secrets else 0o600)
        except FileExistsError:
            pass
    print("Analytics secrets ready; no credential values printed.")
