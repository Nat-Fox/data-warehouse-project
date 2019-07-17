import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Load data from s3 into redshift tables
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data from staging tables, and insert into fact and dimension tables
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """
    for query in insert_table_queries:        
        cur.execute(query)
        conn.commit()
        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()