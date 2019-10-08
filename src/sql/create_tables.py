import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
This script call SQL queries for tables delete and create from sql_queries.py
Creates tables:
- observations (hypertable in timescaleDB)
- node
- sensors
'''

def create_database():
    conn = psycopg2.connect(host="10.0.0.4", port="5342", database="", user="", password="")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
