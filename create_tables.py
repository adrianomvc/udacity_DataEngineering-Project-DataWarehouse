import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables from db_sparkify.
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (db_sparkify).
    Output:
    * Old db_sparkify database tables are dropped from AWS Redshift.
    """
    print("======= DROP TABLE START =======")    
    for query in drop_table_queries:
        try:
            ## Name of Table
            query_lower = query.lower()
            query_lower = query_lower.replace('if exists','')
            name_table_start = query_lower.find('table') + 5
            name_table_end = query_lower.index(';')
            table_name = query_lower[name_table_start:name_table_end].strip()
            print("======= DROP TABLE: {} ".format(table_name))
            
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Drop table: " + query)
            print(e)

    print("======= DROP TABLE DONE =======")


def create_tables(cur, conn):
    """Create new tables (songplays, users, artists, songs, time) to db_sparkify.
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (db_sparkify).
    Output:
    * New db_sparkify database tables are created into AWS Redshift.
    """ 
    print("======= CREATE Table START =======")
    for query in create_table_queries:
        try:
            ## Name of Table
            query_lower = query.lower()
            query_lower = query_lower.replace('if not exists','')
            name_table_start = query_lower.find('table') + 5
            name_table_end = query_lower.index('(')
            table_name = query_lower[name_table_start:name_table_end].strip()
            print("======= CREATE TABLE: {} ".format(table_name))
            
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Create table: " + query)
            print(e)
    print("======= CREATE Table DONE =======")

    
def main():
    """Connect to AWS Redshift, create new DB (db_sparkify),
        drop any existing tables, create new tables. Close DB connection.
    Keyword arguments (from dwh.cfg):
    * host --       AWS Redshift cluster address.
    * dbname --     DB name.
    * user --       Username for the DB.
    * password --   Password for the DB.
    * port --       DB port to connect to.
    * cur --        cursory to connected DB. Allows to execute SQL commands.
    * conn --       (psycopg2) connection to Postgres database (db_sparkify).
    Output:
    * New db_sparkify is created, old tables are droppped,
        and new tables (songplays, users, artists, songs, time)
        are created.
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()