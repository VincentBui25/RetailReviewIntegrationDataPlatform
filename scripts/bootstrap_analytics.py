"""Provision Superset metadata only; preserve all existing databases and roles."""

from pathlib import Path
import psycopg2
from psycopg2 import sql


def secret(name):
    return (Path("/run/secrets") / name).read_text().strip()


if __name__ == "__main__":
    connection = psycopg2.connect(
        host="postgres",
        dbname="postgres",
        user="fnb_admin",
        password=secret("admin_password"),
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        roles = [
            ("fnb_superset_metadata", "superset_metadata_password"),
        ]
        for role, file in roles:
            cursor.execute(
                "select exists(select from pg_roles where rolname=%s)", [role]
            )
            if not cursor.fetchone()[0]:
                cursor.execute(
                    sql.SQL(
                        "create role {} login nosuperuser nocreatedb nocreaterole password %s"
                    ).format(sql.Identifier(role)),
                    [secret(file)],
                )
        databases = [
            ("fnb_superset", "fnb_superset_metadata"),
        ]
        for database, owner in databases:
            cursor.execute(
                "select exists(select from pg_database where datname=%s)", [database]
            )
            if not cursor.fetchone()[0]:
                cursor.execute(
                    sql.SQL("create database {} owner {}").format(
                        sql.Identifier(database), sql.Identifier(owner)
                    )
                )
            cursor.execute(
                sql.SQL("revoke all on database {} from public").format(
                    sql.Identifier(database)
                )
            )
    connection.close()
    print(
        "Superset metadata provisioned; existing analytics/Airflow/state databases preserved."
    )
