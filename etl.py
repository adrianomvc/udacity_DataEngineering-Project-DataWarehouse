import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """Load JSON input data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    Keyword arguments:
    * cur --    reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    Output:
    * log_data in staging_events table.
    * song_data in staging_songs table.
    """
    loadTimes = []
    
    print("Start loading data from S3 to AWS Reshift tables...")
    
    for query in copy_table_queries:
        print("======= LOADING: ==> {} =======".format(query))
        t0 = time()
        
        cur.execute(query)
        conn.commit()
        
        loadTime = time()-t0
        loadTimes.append(loadTime)        
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

    print('All files COPIED OK.')

def insert_tables(cur, conn):
    """Insert data from staging tables (staging_events and staging_songs)
        into star schema analytics tables:
        * Fact table: songplays
        * Dimension tables: users, songs, artists, time
    Keyword arguments:
    * cur --    reference to connected db.
    * conn --   parameters (host, dbname, user, password, port)
                to connect the DB.
    Output:
    * Data inserted from staging tables to dimension tables.
    * Data inserted from staging tables to fact table.
    """
    loadTimes = []
    print("Start inserting data from staging tables into analysis tables...")
    for query in insert_table_queries:
        
        ## Name of Table
        query_lower = query.lower()
        name_table_start = query_lower.index('into') + 4
        name_table_end   = query_lower.index('(')
        table_name = query_lower[name_table_start:name_table_end].strip()
           
        print("======= Insert Table ==> {} ".format(table_name))
        t0 = time()
        
        cur.execute(query)
        conn.commit()
        
        loadTime = time()-t0
        loadTimes.append(loadTime)        
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

    print('All files INSERTED OK.')

def main():
    """Connect to DB and call
        * load_staging_tables to load data from JSON files
            (song_data and log_data in S3) to staging tables and
        * insert_tables to insert data to analysis tables.
    Keyword arguments:
    * None
    Output:
    * All input data processed in DB tables.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("AWS Redshift connection established OK.")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()