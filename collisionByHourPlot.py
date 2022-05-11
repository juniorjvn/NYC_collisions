import os
from datetime import date
from CreateDBConnections import create_postgres_connection


mvc_db = 'motor_vehicle_collision'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
today = date.today()
month = today.strftime('%m')
modzcta = '11207'

conn = create_postgres_connection(user, password, mvc_db)
cur = conn.cursor()

stm = '''
SELECT DATE_PART('hour', crash_time) as hour, ROUND(COUNT(*) / 9.0, 2) as accidents
FROM collisions
WHERE crash_date >= '2013-01-01' and crash_date < '2022-01-01' and
        modzcta = %s and EXTRACT(month FROM crash_date) = %s
GROUP BY hour
ORDER BY hour;
'''


cur.execute(stm, (modzcta, month))
collisions_by_hour = cur.fetchall()

f = open('dailyCollisions.js', 'w')
f.write('dailyCollisions = [["Hour", "' + modzcta + '"]')
for hour in collisions_by_hour:
    f.write(',\n["' + str(int(hour[0])) + ':00", ' + str(hour[1]) + ']')
f.write('\n];\n')
f.close()

conn.close()