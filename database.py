import psycopg as pg                # Note: the module name is psycopg, not psycopg3.
from psycopg.rows import dict_row   # Use this to show columns in data sets returned from postgres db
# TODO get notices from functions

def get_db(host: str):
    db_host = host
    db_name = 'simplicity'
    db_user = 'postgres'
    conn_string = f"host={db_host} dbname={db_name} user={db_user}"

    conn = pg.connect(conn_string, row_factory=dict_row)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def get_db2(host: str):
    db_host = host
    db_name = 'simplicity'
    db_user = 'postgres'
    conn_string2 = f"host={db_host} dbname={db_name} user={db_user}"

    conn2 = pg.connect(conn_string2, row_factory=dict_row)
    try:
        yield conn2
    finally:
        conn2.close()