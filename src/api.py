from aot_client import AotClient, F
import psycopg2
import ciso8601
import time
import datetime
from sql_queries import *

# Get previous record timestamp
try:
    fh = open("state.txt", "r")
    prev_record_timestamp = fh.read()
    t = ciso8601.parse_datetime(prev_record_timestamp)
    fh.close()
except FileNotFoundError:
    t = (datetime.datetime.utcnow() - datetime.timedelta(minutes=15))
    prev_record_timestamp = t.isoformat()[0:19]

# Init client
client = AotClient()

# Init filter
f = F('project', 'chicago')
f &= ('size', '5000')
f &= ('timestamp', 'gt', prev_record_timestamp)
f &= ('order', 'asc:timestamp')

# # Prepare insert statement
sql = observations_table_insert
# observations_table_insert = "INSERT INTO aotdb.observations (ts, sensor_path, value_hrf) VALUES (%s,%s,%s)"
try:
    page_num = 1
    inserted = 0

    # Connect to DB
    conn = psycopg2.connect(host="10.0.0.4", port="5342", database="aotdb", user="pgadmin", password="n*tAdmin85")
    cur = conn.cursor()

    # Get observations
    observations = client.list_observations(filters=f)

    # Iterate through records
    for page in observations:
        print(f'Page {page_num}')
        to_insert = []
        for obs in page.data:
            ts = ciso8601.parse_datetime(obs["timestamp"])
            prev_record_timestamp = obs["timestamp"]

            to_insert.append((time.mktime(ts.timetuple()), obs["sensor_path"], obs["value"]))

        # If this array is empty it means that we skipped all the records
        if len(to_insert) == 0:
            break

        cur.executemany(observations_table_insert, to_insert)
        conn.commit()
        page_num += 1
        inserted += len(to_insert)
        print(f'Inserted {inserted} records so far')
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

    fh = open("state.txt", "w+")
    fh.write(prev_record_timestamp)
    fh.close()

print(f'Inserted {inserted} records total')

# def main():
#     """
#     Description: main function is executed first when script api.py is lunched.
#     Establishes and closes connection to database,
#
#     Arguments:
#         None
#
#     Returns:
#         None
#     """
#     conn = psycopg2.connect(host="10.0.0.4", port="5342", database="aotdb", user="pgadmin", password="n*tAdmin85")
#     cur = conn.cursor()
    # conn.close()


# if __name__ == "__main__":
#     main()
