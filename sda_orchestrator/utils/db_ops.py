"""Database operation for mapping file ID to dataset ID."""
from typing import Dict
import psycopg2
from .logger import LOG
from time import sleep
import os
from pathlib import Path
from psycopg2.extras import DictCursor


class DB:
    """DB class encapsulating a psycopg2 connection."""

    __state: Dict = {}

    user = ""
    password = ""  # nosec

    def __init__(self) -> None:
        """Init DB class."""
        self.__dict__ = self.__state
        if not hasattr(self, "conn"):
            self.conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                database=os.environ.get("DB_DATABASE", "lega"),
                host=os.environ.get("DB_HOST", "localhost"),
                sslmode=os.environ.get("DB_SSLMODE", "require"),
                port=os.environ.get("DB_PORT", 5432),
                sslrootcert=Path(f"{os.environ.get('SSL_CACERT', '/tls/certs/ca.crt')}"),
                sslcert=Path(f"{os.environ.get('SSL_CLIENTCERT', '/tls/certs/orch.crt')}"),
                sslkey=Path(f"{os.environ.get('SSL_CLIENTKEY', '/tls/certs/orch.key')}"),
            )
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.close = self.conn.close()
            self.commit = self.conn.commit()


def map_file2dataset(user: str, filepath: str, decrypted_checksum: str, dataset_id: str) -> None:
    """Assign file to dataset, for dataset driven permissions.

    We establish 2 connections to db one to check the file has been marked READY or DISABLED.
    If the file is in neither of this statuses after verify that means we need to wait for
    accession ID
    ERROR status should have occured sooner.

    After this we get all the files matching a user, path and checksum.
    We map in the Data Out table the accession ID to a dataset ID.
    """
    conn = DB()
    conn.user = os.environ.get("DB_IN_USER", "lega_out")
    conn.password = os.environ.get("DB_IN_PASSWORD", "")

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
                    if f[0] not in ("READY", "DISABLED"):
                        LOG.debug(f"Waiting for status to be READY or DISABLED {f[0]}")
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

    last_index = None
    # table out data out requires a different user, thus also a different connection

    conn.user = os.environ.get("DB_OUT_USER", "lega_out")
    conn.password = os.environ.get("DB_OUT_PASSWORD", "")

    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM local_ega_ebi.filedataset ORDER BY id DESC LIMIT 1")
        value = cursor.fetchone()
        last_index = value[0] if value is not None else 0

    with conn.cursor() as cursor:
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
            conn.commit()
    conn.close()
