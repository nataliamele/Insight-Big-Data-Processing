# DROP TABLES

observations_table_drop = "DROP TABLE IF EXISTS observations;"
nodes_table_drop = "DROP TABLE IF EXISTS nodes;"
sensors_table_drop = "DROP TABLE IF EXISTS sensors;"

# CREATE TABLES

# Fact table - streaming data from sensors
observations_table_create = ("CREATE TABLE IF NOT EXISTS observation (\
                                record_id SERIAL PRIMARY KEY, \
                                timestamp_utc INT, \
                                node_id TEXT REFERENCES nodes(node_id), \
                                sensor_path TEXT REFERENCES sensors(sensor_path), \
                                value_hrf FLOAT
                            );")

#Dimensions tables - nodes and sensors:

sensors_table_create = ("CREATE TABLE IF NOT EXISTS sensors (\
                          sensor_path TEXT PRIMARY KEY, \
                          sensor_type TEXT, \
                          sensor_measure TEXT, \
                          hrf_unit TEXT, \
                          hrf_min NUMERIC, \
                          hrf_max NUMERIC
                    );")

nodes_table_create = ("CREATE TABLE IF NOT EXISTS nodes( \
                      node_id TEXT PRIMARY KEY, \
                      lat NUMERIC, \
                      lon NUMERIC, \
                      community_area TEXT, \
                      description TEXT
                    ;")

# INSERT RECORDS

observations_table_insert = ("INSERT INTO observsation( \
                    record_id, timestamp_utc, node_id, sensor_path, value_hrf) \
                    VALUES (%s, %s, %s, %s, %s)")

sensors_table_insert = ("INSERT INTO sensors(sensors_path, sensor_type, sensor_measure, hrf_unit, hrf_min, hrf_max) \
                    VALUES (%s, %s, %s, %s, %s, %s)")

nodes_table_insert = ("INSERT INTO nodes(node_id, lat, lon, community_area, description) VALUES (%s, %s, %s, %s, %s) \
                    ON CONFLICT (node_id) DO NOTHING;")

# QUERIES LISTS

create_table_queries = [observations_table_create, sensors_table_create, nodes_table_create, ]
drop_table_queries = [observations_table_drop, sensors_table_drop, nodes_table_drop, ]
