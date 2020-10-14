"""Database operation for mapping file ID to dataset ID."""
from typing import Any, Dict, Union
import psycopg2
from .logger import LOG
from time import sleep
import os
from pathlib import Path
from .id_ops import generate_dataset_id


class DBConnector:
    """DBConnector general class."""

    def __init__(self, user: Union[str, None], password: Union[str, None]) -> None:
        """Define init parameters."""
        self.user = user
        self.password = password
        self.dbconn = None

    def create_connection(self) -> psycopg2.connect:
        """Create connection."""
        db_config = {
            "user": self.user,
            "password": self.password,
            "database": os.environ.get("DB_DATABASE", "lega"),
            "host": os.environ.get("DB_HOST", "localhost"),
            "sslmode": os.environ.get("DB_SSLMODE", "require"),
            "port": os.environ.get("DB_PORT", 5432),
            "sslrootcert": Path(f"{os.environ.get('SSL_CACERT', '/tls/certs/ca.crt')}"),
            "sslcert": Path(f"{os.environ.get('SSL_CLIENTCERT', '/tls/certs/orch.crt')}"),
            "sslkey": Path(f"{os.environ.get('SSL_CLIENTKEY', '/tls/certs/orch.key')}"),
        }
        LOG.debug("connecting to PostgreSQL database...")
        connection = psycopg2.connect(**db_config)

        return connection

    def __enter__(self) -> None:
        """Create connection enter."""
        self.dbconn = self.create_connection()
        return self.dbconn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close connection exit."""
        self.dbconn.close()  # type:ignore


class DBConnection:
    """DBConnection class for using singleton db connnection."""

    connection = None

    @classmethod
    def get_connection(
        cls: psycopg2.connect, new: bool = False, user: Union[str, None] = None, passwd: Union[str, None] = None
    ) -> psycopg2.connect:
        """Return new Singleton database connection."""
        if new or not cls.connection:
            cls.connection = DBConnector(user, passwd).create_connection()
        LOG.debug("connection established")
        return cls.connection

    @classmethod
    def execute_query(
        cls: psycopg2.connect, user: Union[str, None], passwd: Union[str, None], query: str, params: dict
    ) -> Dict:
        """Execute query on singleton db connection."""
        connection = cls.get_connection(new=True, user=user, passwd=passwd)
        cursor = connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        cursor.close()
        return result

    @classmethod
    def insert_query(
        cls: psycopg2.connect, user: Union[str, None], passwd: Union[str, None], query: str, params: dict
    ) -> None:
        """Execute insert query on singleton db connection."""
        connection = cls.get_connection(new=True, user=user, passwd=passwd)
        cursor = connection.cursor()
        cursor.execute(query, params)
        connection.commit()
        cursor.close()

    @classmethod
    def execute_query_one(
        cls: psycopg2.connect, user: Union[str, None], passwd: Union[str, None], query: str, params: Union[dict, None]
    ) -> Dict:
        """Execute query for one value on singleton db connection."""
        connection = cls.get_connection(new=True, user=user, passwd=passwd)
        cursor = connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        cursor.close()
        return result


def map_file2dataset(user: str, filepath: str, decrypted_checksum: str) -> None:
    """Assign file to dataset, for dataset driven permissions.

    We establish 2 connections to db one to check the file has been marked READY or DISABLED.
    If the file is in neither of this statuses after verify that means we need to wait for
    accession ID
    ERROR status should have occured sooner.

    After this we get all the files matching a user, path and checksum.
    We map in the Data Out table the accession ID to a dataset ID.
    """
    conn = DBConnection()
    in_user = os.environ.get("DB_IN_USER", "lega_in")
    in_passwd = os.environ.get("DB_IN_PASSWORD", "")
    sleep_time = 2
    num_retries = 5
    for x in range(0, num_retries):
        files = conn.execute_query(
            in_user,
            in_passwd,
            "SELECT status FROM local_ega.files where elixir_id = %(user)s AND archive_path = %(filepath)s"
            " AND archive_file_checksum = %(decrypted_checksum)s",
            {"user": user, "filepath": filepath, "decrypted_checksum": decrypted_checksum},
        )
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

    LOG.info("checked has correct status in DB.")

    archived_files = conn.execute_query(
        in_user,
        in_passwd,
        "SELECT id, inbox_path FROM local_ega.files where elixir_id = %(user)s AND archive_path = %(filepath)s"
        " AND archive_file_checksum = %(decrypted_checksum)s",
        {"user": user, "filepath": filepath, "decrypted_checksum": decrypted_checksum},
    )
    LOG.debug(f"retrieved id {archived_files} for files with archive file {filepath} from DB.")

    last_index = None
    # table out data out requires a different user, thus also a different connection
    out_user = os.environ.get("DB_OUT_USER", "lega_out")
    out_passwd = os.environ.get("DB_OUT_PASSWORD", "")

    value = conn.execute_query_one(
        out_user, out_passwd, "SELECT id FROM local_ega_ebi.filedataset ORDER BY id DESC LIMIT 1", None
    )
    last_index = value[0] if value is not None else 0

    for f in archived_files:
        dataset_id = generate_dataset_id(user, f[1])
        conn.insert_query(
            out_user,
            out_passwd,
            """INSERT INTO local_ega_ebi.filedataset(id, file_id, dataset_stable_id)
            VALUES(%(last_index)s, %(file_id)s, %(dataset_id)s)""",
            {
                "last_index": last_index + 1 if last_index is not None else 1,
                "file_id": f[0],
                "dataset_id": dataset_id,
            },
        )
        last_index += 1
        LOG.info(f"Mapped file with ID: {f[0]} to Dataset: {dataset_id}")
