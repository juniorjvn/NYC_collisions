import json
import requests
import psycopg2
import re
import os
from CreateDBConnections import create_postgres_connection


# This program will create boroughs, modzctas(modified zip code tabulation ares), and zipcodes tables
# in motor_vehicle_collision database

database = 'motor_vehicle_collision'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
borough_api = 'https://data.cityofnewyork.us/resource/7t3b-ywvw.json'
zipcode_api = 'https://data.cityofnewyork.us/resource/pri4-ifjk.json'

conn = create_postgres_connection(user=user, password=password, database=database)
cur = conn.cursor()

# Create boroughs table
create_borough_table_stm = '''
DROP TABLE IF EXISTS boroughs;
CREATE TABLE boroughs(
    borough_id SERIAL,
    name VARCHAR(15),
    geom TEXT,
    PRIMARY KEY(borough_id)
);
'''
cur.execute(create_borough_table_stm)
conn.commit()

response = requests.get(borough_api)
# If data cannot be retrieved, the program will exit
if response.status_code != 200:
    print("Error code", response.status_code, borough_api)
    exit()

# rows keys 'the_geom', 'boro_code', 'boro_name', 'shape_leng', 'shape_area'
rows = response.json()
for row in rows:
    insert_stm = '''INSERT INTO boroughs(borough_id, name, geom) VALUES(%s, %s, %s);'''
    try:
        cur.execute(insert_stm, (row['boro_code'], row['boro_name'], json.dumps(row['the_geom'])))
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        print('Error inserting values {} into Postgresql DB.'.format(row))
        print('Error:', e)
        exit()

conn.commit()
print('Table boroughs was successfully created')

# Create zipcodes table
create_zipcode_table_stm = '''
DROP TABLE IF EXISTS zipcodes;
CREATE TABLE zipcodes(
    zipcode VARCHAR(5),
    modzcta VARCHAR(5),
    PRIMARY KEY(zipcode)
);

DROP TABLE IF EXISTS modzctas;
CREATE TABLE modzctas(
    modzcta VARCHAR(5),
    population DECIMAL,
    geom TEXT,
    PRIMARY KEY(modzcta)
);

'''
cur.execute(create_zipcode_table_stm)
conn.commit()

response = requests.get(zipcode_api)
# If data cannot be retrieved, the program will exit
if response.status_code != 200:
    print("Error code", response.status_code, borough_api)
    exit()

# rows keys 'modzcta', 'label', 'zcta', 'pop_est', 'the_geom'
rows = response.json()
counter = 0
for row in rows:
    modzcta = row['modzcta']
    # get all available zip codes and remove duplicate items
    zipcodes = re.findall('\d+', row['label'])
    zipcodes += re.findall('\d+', row['zcta'])
    zipcodes = set(zipcodes)

    # Insert all available zip codes to zipcodes table
    try:
        insert_zipcode_stm = '''INSERT INTO zipcodes(zipcode, modzcta) VALUES(%s, %s) ON CONFLICT DO NOTHING;'''
        for zipcode in zipcodes:
            cur.execute(insert_zipcode_stm, (zipcode, modzcta))

        insert_modzctd_stm = '''INSERT INTO modzctas(modzcta, population, geom) VALUES(%s, %s, %s) ON CONFLICT DO NOTHING;'''
        cur.execute(insert_modzctd_stm, (modzcta, row['pop_est'], json.dumps(row['the_geom'])))
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        print('Error inserting values {} into Postgresql DB.'.format(row))
        print('Error:', e)
        exit()

conn.commit()
conn.close()
print('Table zipcodes ans modzctas were successfully created')


