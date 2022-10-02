import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - loads data from the sparkify S3bucket into our staging tables.
        (args)
    
        conn: the connection to the database.
        cur: the cursor connection to our database.
        """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - inserts the data from our 2 staging tables into the 1 fact vs 4 dimension final tables.
        (args)
    
        conn: the connection to the database.
        cur: the cursor connection to our database.
        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - access the config file when needed for credentials and endpoint to execute both functions above.
        """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()