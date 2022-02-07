import psycopg2
import os

fileloc = 's3_data/data-bucket/'
files = os.listdir(fileloc)
csvs = []
for i in files:
    if i[-4:] == '.csv':
        csvs.append(fileloc + i)

conn = psycopg2.connect(database="postgres",
                        user='postgres', password='password', 
                        host='localhost', port='5432'
)

conn.autocommit = True
cur = conn.cursor()

for i in csvs:
    sqlstr = "COPY game_stats FROM STDIN DELIMITER ',' CSV header;"
    with open(i) as f:
        cur.copy_expert(sqlstr, f)
    conn.commit()
