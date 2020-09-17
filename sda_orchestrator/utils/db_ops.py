"""Database operation for mapping file ID to dataset ID."""
import psycopg2
from .logger import LOG
from time import sleep
import os
from pathlib import Path


def map_file2dataset(user, filepath, decrypted_checksum, dataset_id):
    """Assign file to dataset, for dataset driven permissions."""
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
        # wait for file satus to be ready
        sleep_time = 2
        num_retries = 5
        for x in range(0, num_retries):
            try:
                for f in files:
                    if f[0] != "READY" and f[0] != "DISABLED":
                        LOG.info(f"trying {f[0]}")
                        raise Exception
            except Exception as str_error:
                if str_error:
                    sleep(sleep_time)  # wait before trying to fetch the data again
                    sleep_time *= 2  # Implement your backoff algorithm here i.e. exponential backoff
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
    # table in data out requires a different user, thus also a different connection
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
                "INSERT INTO local_ega_ebi.filedataset(id, file_id, dataset_stable_id) VALUES(%(last_index)s, %(file_id)s, %(dataset_id)s)",
                {
                    "last_index": last_index + 1 if last_index is not None else 1,
                    "file_id": f[0],
                    "dataset_id": dataset_id,
                },
            )
            last_index += 1
            LOG.debug(f"Mapped ID: {f[0]} to Dataset: {dataset_id}")
            conn2.commit()
    conn2.close()


def retrieve_file2dataset():
    """Assign file to dataset, for dataset driven permissions."""
    conn = psycopg2.connect(
        user=os.environ.get("DB_OUT_USER", "lega_out"),
        password=os.environ.get("DB_OUT_PASSWORD"),
        database=os.environ.get("DB_NAME", "lega"),
        host=os.environ.get("DB_HOST", "localhost"),
        sslmode="require",
        sslrootcert=Path("/tls/certs/root.ca.crt"),
        sslcert=Path("/tls/certs/cert.ca.crt"),
        sslkey=Path("/tls/certs/cert.ca.key"),
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT file_id, dataset_id FROM local_ega_ebi.file_dataset WHERE file_id is NOT NULL")
        matches = cursor.fetchall()
    conn.close()

    return matches
