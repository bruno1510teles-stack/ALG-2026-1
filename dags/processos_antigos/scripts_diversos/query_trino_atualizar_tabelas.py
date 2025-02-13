import trino
import progressbar
from multiprocessing.pool import ThreadPool
import time
import pandas as pd

def query_trino(query, catalog, host, port, user, password):

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        http_scheme='https',
        catalog=catalog,
        auth=trino.auth.BasicAuthentication(
                            user, 
                            password),
    )

    cur = conn.cursor()
    cur.execute(query)

    # get a cur (cursor) object from above
    cur.fetchall()