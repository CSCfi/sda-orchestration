"""Database operation for mapping file ID to dataset ID."""
import psycopg2
from .logger import LOG
from time import sleep
import os
from pathlib import Path


def map_file2dataset(user: str, filepath: str, decrypted_checksum: str, dataset_id: str) -> None:
    """Assign file to dataset, for dataset driven permissions.

    We establish 2 connections to db one to check the file has been marked READY or DISABLED.
    If the file is in neither of this statuses after verify that means we need to wait for
    accession ID
    ERROR status should have occured sooner.

    After this we get all the files matching a user, path and checksum.
    We map in the Data Out table the accession ID to a dataset ID.
    """
    conn = psycopg2.connect(
        user=os.environ.get("DB_IN_USER", "lega_in"),
        password=os.environ.get("DB_IN_PASSWORD"),
        database=os.environ.get("DB_NAME", "lega"),
        host=os.environ.get("DB_HOST", "localhost"),
        sslmode="require",
        sslrootcert=Path("/tls/certs/root.ca.crt"),
        sslcert=Path("/tls/certs/cert.ca.crt"),
        sslkey=Path("/tls/certs/cert.ca.key"),
    )
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT status FROM local_ega.files where elixir_id = %(user)s AND inbox_path = %(filepath)s",
            {"user": user, "filepath": filepath},
        )
        files = cursor.fetchall()
        # wait for file status to be ready or disabled
        sleep_time = 2
        num_retries = 5
        for x in range(0, num_retries):
            try:
                for f in files:
                    if f[0] != "READY" and f[0] != "DISABLED":
                        LOG.debug(f"Waiting to status to be COMPLETED or DISABLED {f[0]}")
                        raise Exception
            except Exception as str_error:
                if str_error:
                    sleep(sleep_time)
                    sleep_time *= 2
                else:
                    break

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM local_ega.files where elixir_id = %(user)s AND inbox_path = %(filepath)s"
            " AND archive_file_checksum = %(decrypted_checksum)s",
            {"user": user, "filepath": filepath, "decrypted_checksum": decrypted_checksum},
        )
        files = cursor.fetchall()
    conn.close()

    last_index = None
    # table out data out requires a different user, thus also a different connection
    conn2 = psycopg2.connect(
        user=os.environ.get("DB_OUT_USER", "lega_out"),
        password=os.environ.get("DB_OUT_PASSWORD"),
        database=os.environ.get("DB_NAME", "lega"),
        host=os.environ.get("DB_HOST", "localhost"),
        sslmode="require",
        sslrootcert=Path("/tls/certs/root.ca.crt"),
        sslcert=Path("/tls/certs/cert.ca.crt"),
        sslkey=Path("/tls/certs/cert.ca.key"),
    )

    with conn2.cursor() as cursor:
        cursor.execute("""SELECT id FROM local_ega_ebi.filedataset ORDER BY id DESC LIMIT 1""")
        value = cursor.fetchone()
        last_index = value[0] if value is not None else 0

    with conn2.cursor() as cursor:
        for f in files:
            cursor.execute(
                """INSERT INTO local_ega_ebi.filedataset(id, file_id, dataset_stable_id)
                VALUES(%(last_index)s, %(file_id)s, %(dataset_id)s)""",
                {
                    "last_index": last_index + 1 if last_index is not None else 1,
                    "file_id": f[0],
                    "dataset_id": dataset_id,
                },
            )
            last_index += 1
            LOG.info(f"Mapped Accession ID: {f[0]} to Dataset: {dataset_id}")
            conn2.commit()
    conn2.close()
