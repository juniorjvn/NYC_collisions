import psycopg2
import requests
import os
from CreateDBConnections import create_postgres_connection

db_name = 'nyc_open_data'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
first_record_date = '2012-07-01'
table_name = 'Collisions'
base_api = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$select=*&$where=crash_date>="{}" and crash_time>="{}"&$order=crash_date, crash_time&$limit={}'

# Connect to the newly created database nyc_open_data and create a table
conn = create_postgres_connection(user=user, password=password, database=db_name)
cur = conn.cursor()

create_table_collisions_stm = '''
CREATE TABLE IF NOT EXISTS {}(
    collision_id SERIAL,
    borough VARCHAR(15),
    contributing_factor_vehicle_1 VARCHAR(250),
    contributing_factor_vehicle_2 VARCHAR(250),
    contributing_factor_vehicle_3 VARCHAR(250),
    contributing_factor_vehicle_4 VARCHAR(250),
    contributing_factor_vehicle_5 VARCHAR(250),
    crash_date DATE,
    crash_time TIME,
    cross_street_name VARCHAR(250),
    location VARCHAR(250),
    latitude FLOAT(20) ,
    longitude FLOAT(20),
    number_of_cyclist_injured DECIMAL(3),
    number_of_cyclist_killed DECIMAL(3),
    number_of_motorist_injured DECIMAL(3),
    number_of_motorist_killed DECIMAL(3),
    number_of_pedestrians_injured DECIMAL(3),
    number_of_pedestrians_killed DECIMAL(3),
    number_of_persons_injured DECIMAL(3),
    number_of_persons_killed DECIMAL(3),
    off_street_name VARCHAR(250),
    on_street_name VARCHAR(250),
    vehicle_type_code1 VARCHAR(100),
    vehicle_type_code2 VARCHAR(100),
    vehicle_type_code_3 VARCHAR(100),
    vehicle_type_code_4 VARCHAR(100),
    vehicle_type_code_5 VARCHAR(100),
    zip_code VARCHAR(10),
    PRIMARY KEY(collision_id)
);
'''

cur.execute(create_table_collisions_stm.format(table_name, table_name))
conn.commit()
# print('Table "{}" was successfully created'.format(table_name))


# import new record into collisions db
# Number or record to be inserted
num_of_records = int(input('how many records: '))
crush_date = first_record_date
crush_time = "0"

# Looking for the latest record inserted
cur.execute('''SELECT crash_date, crash_time FROM collisions ORDER BY crash_date DESC, crash_time DESC LIMIT 1;''')
latest_date = cur.fetchone()

if latest_date:
    crush_date = latest_date[0].strftime("%y-%m-%d")
    crush_time = latest_date[1].strftime("%H:%M")
    num_of_records += 1

# request data from NYC Open Data api
print("Retrieving data...")
api = base_api.format(crush_date, crush_time, num_of_records)
response = requests.get(api)

# If data cannot be retrieved, the program will exit
if response.status_code != 200:
    print("Error code", response.status_code, api)
    exit()


insert_stm = '''INSERT INTO collisions({}) VALUES({}) ON CONFLICT DO NOTHING RETURNING collision_id;'''
counter = 0
rows = response.json()

if rows:
    print("Inserting new records into collisions table...")

    for row in rows:
        # If location is found in the dictionary, only latitude and longitude will be extracted
        location = row.get('location', None)
        if location:
            row['location'] = str((location['latitude'], location['longitude']))

        columns = ','.join(list(row.keys()))

        # Inserting values as a tuple parameter to avoid error inserting quotes inside strings
        values_to_insert = '%s,' * len(row)
        values = tuple(map(str.strip, row.values()))
        try:
            cur.execute(insert_stm.format(columns, values_to_insert[:-1]), values)
            valid_id = cur.fetchone()
            if valid_id:
                counter += 1
        except psycopg2.Error as e:
            conn.rollback()
            conn.close()
            print('Error inserting values {} into Postgresql DB.'.format(row))
            print('Error:', e)
            exit()

conn.commit()

if counter == 0:
    print('\nThe Database is up to date.')
else:
    print('\nNew collision records were successfully inserted')







