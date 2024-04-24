import trino
import progressbar
from multiprocessing.pool import ThreadPool
import time
import pandas as pd

def query_trino(query, host, port, user, password):
    
    print('criando conexão')
    
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        http_scheme='https',
        catalog='postgres',
        schema='pg_catalog',
        auth=trino.auth.BasicAuthentication(
                            user, 
                            password),
    )

    print('conectando')
    cur = conn.cursor()
    cur.execute(query)

    def fetch_rows(cur):
        pool = ThreadPool(processes=1)
        async_result = pool.apply_async(cur.fetchall, ())
        
        #initialise progressbar
        bar = progressbar.ProgressBar(maxval=100, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar_start = False
        
        while not async_result.ready():
            if cur.stats['scheduled']:
                if not bar_start:
                    bar.start()
                    bar_start=True
                perc = round((cur.stats['completedSplits']*100.0)/(cur.stats['totalSplits']),2)
                bar.update(int(perc))
                time.sleep(1)
            else:
                perc = '0'
                print(cur.stats['state']+'-'+str(perc)+'%')
                time.sleep(1)
        if bar_start:
            bar.finish()
        print(cur.stats['state']+'-'+str(cur.stats.get('progressPercentage','')))
        
        return_val = async_result.get()
        return return_val

    print('executando query')
    # get a cur (cursor) object from above
    rows = fetch_rows(cur)
    
    print('definindo nome da coluna no DF')
    columns = [x[0] for x in cur.description]
    
    print('Transformando em Df')
    result = pd.DataFrame(rows, columns=columns)

    return result