"""Create local secrets once; optionally rotate an existing development DB role."""

import argparse
import os
from pathlib import Path
import secrets


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rotate-existing-db", action="store_true")
    args = parser.parse_args()
    target = Path(__file__).resolve().parents[1] / ".env"
    if target.exists():
        raise SystemExit(".env already exists; preserved it")
    values = {
        "POSTGRES_PASSWORD": secrets.token_hex(24),
        "MINIO_ROOT_USER": "cataclysm-local",
        "MINIO_ROOT_PASSWORD": secrets.token_hex(24),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
        "OPENWEATHER_API_KEY": "",
    }
    if args.rotate_existing_db:
        import psycopg
        from psycopg import sql

        with psycopg.connect(os.environ["CURRENT_DATABASE_URL"]) as conn:
            conn.execute(
                sql.SQL("ALTER ROLE cataclysm PASSWORD {}").format(
                    sql.Literal(values["POSTGRES_PASSWORD"])
                )
            )
    with target.open("x", encoding="utf-8") as stream:
        stream.write(
            "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
        )
    print("Created ignored .env with random local secrets; weather key is empty")


if __name__ == "__main__":
    main()
