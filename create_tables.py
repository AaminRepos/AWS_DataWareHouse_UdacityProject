import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - drop tables if the exist in the redshift cluster.
        (args)
    
        conn: the connection to the database.
        cur: the cursor connection to our database.
        """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - creates the tables if in the redshift cluster.
        (args)
    
        conn: the connection to the database.
        cur: the cursor connection to our database.
        """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()