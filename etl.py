import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Read data from S3 buckets and load the data into staging tables.
    """
    for query in copy_table_queries:
        print('------------------')
        print('Processing query:\n------------------\n {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('Processed OK.')


def insert_tables(cur, conn):
    """
    Read from staging tables, transform data and 
    insert into fact/dimention tables for the data analysis.
    """
    for query in insert_table_queries:
        print('------------------')
        print('Processing query:\n------------------\n {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('Processed OK.\n******************')


def main():
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