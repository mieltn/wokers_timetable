# from SQLAlchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import DATABASE, USER


# создание базы данных timetable
conn = psycopg2.connect(
    database = DATABASE,
    user = USER
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()
create_db = 'CREATE DATABASE timetable'

cursor.execute(create_db)

conn.close()


# создание таблицы 1 timetable_old
conn = psycopg2.connect(
    database = 'timetable',
    user = 'postgres'
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()
create_table = '''
CREATE TABLE timetable_old (
    tabnum INT NOT NULL,
    start_date DATE NOT NULL,
    finish_date DATE NOT NULL,
    wday_type01 INT NOT NULL,
    wday_type02 INT NOT NULL,
    wday_type03 INT NOT NULL,
    wday_type04 INT NOT NULL,
    wday_type05 INT NOT NULL,
    wplace_type INT NOT NULL,
    end_da DATE
)
'''

cursor.execute(create_table)

# наполнение данными из таблицы 1
data = [
    [15123, '2020-09-02', '9999-12-31', 0, 0, 0, 0, 0, 0, '2020-10-31'],
    [16234, '2020-09-20', '2020-10-30', 0, 0, 1, 1, 0, 2, None],
    [17345, '2020-09-28', '2020-10-25', 1, 0, 0, 0, 0, 2, None],
    [17345, '2020-10-26', '2020-12-31', 1, 1, 1, 1, 1, 1, None],
    [18456, '2020-09-02', '9999-12-31', 2, 2, 2, 2, 2, 3, '2020-09-30'],
    [19567, '2020-09-02', '2020-12-31', 3, 3, 3, 3, 3, 4, None]
]

for row in data:
    populate = '''
    INSERT INTO timetable_old (
        tabnum, start_date, finish_date,
        wday_type01, wday_type02, wday_type03, wday_type04, wday_type05,
        wplace_type, end_da
    )

    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    '''

    cursor.execute(populate, row)

conn.close()