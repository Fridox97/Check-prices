import pymysql
import paramiko
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from os.path import expanduser

pd.options.display.max_columns = None


# Función para obtener todos los productos de superkiwi.
def get_products(db, query, field):
    home = expanduser('~')
    mypkey = paramiko.RSAKey.from_private_key_file(db.pem)

    with SSHTunnelForwarder(
            (db.ssh_host, db.ssh_port),
            ssh_username=db.ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(db.sql_hostname, db.sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=db.sql_username,
                               passwd=db.sql_password, db=db.sql_main_database,
                               port=tunnel.local_bind_port)
        # query = 'SELECT product_upc FROM tbl_products;'
        data = pd.read_sql_query(query, conn, dtype=str)

        if field != "none":
            # Removiendo todos los productos que no contengan upc.
            for dat in range(len(data)):
                if data.at[dat, field] == '':
                    data = data.drop(dat)

        conn.close()
        return data


def update(db, statement):
    home = expanduser('~')
    mypkey = paramiko.RSAKey.from_private_key_file(db.pem)

    with SSHTunnelForwarder(
            (db.ssh_host, db.ssh_port),
            ssh_username=db.ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(db.sql_hostname, db.sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=db.sql_username,
                               passwd=db.sql_password, db=db.sql_main_database,
                               port=tunnel.local_bind_port,
                               cursorclass=pymysql.cursors.DictCursor,
                               autocommit=True)
        # query = 'SELECT product_upc FROM tbl_products;'
        try:
            cursorObject = conn.cursor()
            cursorObject.execute(statement)

        finally:
            conn.close()
