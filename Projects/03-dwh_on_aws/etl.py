import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables (song data and event data)

    Parameters
    ----------
        cur: cursor from a psycopg2 connection to the Redshift cluster
        conn: the psycopg2 connection to the Redshift cluster
    """

    print('Inserting data into staging tables...')
    for query in copy_table_queries:
        print("...")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables to fact and dimension tables

    Parameters
    ----------
        cur: cursor from a psycopg2 connection to the Redshift cluster
        conn: the psycopg2 connection to the Redshift cluster
    """

    print('Inserting data into analytic tables...')
    for query in insert_table_queries:
        print("...")
        cur.execute(query)
        conn.commit()


def main():
    """
    Load data from S3 to staging tables and then
    insert data from staging tables to fact and dimension tables
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connection to database...')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Connected...')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
