import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    this function to drop all tables in drop_table_queries
    two args
        cur: the cursor
        conn:connection to database
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
    this function to create all tables in create_table_queries
    two agrs
        cur: the cursor
        conn:connection to database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    this func to implement all function in in order
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(*config['CLUSTER'].values())

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
