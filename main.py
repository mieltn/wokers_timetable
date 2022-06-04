import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import DB, DEFAULT_DB, USER


def connect(db, user):
    '''
    Establishing connection to database.
    '''

    # подключение к базе
    conn = psycopg2.connect(
        database = db,
        user = user
    )
    # установка автокомита после транзакций
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    return conn


def create():
    '''
    Function that creates database timetable.
    Then creates and populates table timtable_old with given data.
    '''

    # подключение к дефолтной базе
    conn = connect(DEFAULT_DB, USER)

    # создание курсора, выполнение запроса на создание базы timetable
    cursor = conn.cursor()
    create_db = 'CREATE DATABASE timetable'
    cursor.execute(create_db)

    # завершение подключения
    conn.close()


    # создание таблицы 1 timetable_old
    # подключение к созданной базе
    conn = connect(DB, USER)

    # создание курсора, выполнение запроса на создание таблицы
    cursor = conn.cursor()
    create_table = '''
    CREATE TABLE timetable_old (
        tab_num INT NOT NULL,
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
    # каждый список - строка исходной таблицы
    data = [
        [15123, '2020-09-02', '9999-12-31', 0, 0, 0, 0, 0, 0, '2020-10-31'],
        [16234, '2020-09-20', '2020-10-30', 0, 0, 1, 1, 0, 2, None],
        [17345, '2020-09-28', '2020-10-25', 1, 0, 0, 0, 0, 2, None],
        [17345, '2020-10-26', '2020-12-31', 1, 1, 1, 1, 1, 1, None],
        [18456, '2020-09-02', '9999-12-31', 2, 2, 2, 2, 2, 3, '2020-09-30'],
        [19567, '2020-09-02', '2020-12-31', 3, 3, 3, 3, 3, 4, None]
    ]

    # построчная запись timetable_old
    for row in data:
        populate = '''
        INSERT INTO timetable_old (
            tab_num, start_date, finish_date,
            wday_type01, wday_type02, wday_type03, wday_type04, wday_type05,
            wplace_type, end_da
        )

        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        '''

        cursor.execute(populate, row)

    # завершение подключения
    conn.close()

    return 'ok'


def transform():
    '''
    Function that transforms timetable_old to needed form and saves result to another table.
    '''

    # подключение к созданной базе
    conn = connect(DB, USER)

    cursor = conn.cursor()

    transform_table = '''
    SELECT * INTO timetable_new
    FROM (
    -- cte в которой генерируется временной ряд
    -- через cross join получаем все сочетания дат и пользователя
    WITH dates_cte AS (
        SELECT dt.date, tn.tab_num
        FROM generate_series('2020-09-01', '2020-12-31', interval '1 day') AS dt
        CROSS JOIN (SELECT DISTINCT tab_num FROM timetable_old) AS tn
    ),

    -- cte в которой заполняем finish_date атрибутом end_da, где такой есть
    t_old_cte AS (
        SELECT
            tab_num,
            start_date,
            CASE
                WHEN end_da IS NOT NULL THEN end_da
                ELSE finish_date
            END AS finish_date,
            wday_type01,
            wday_type02,
            wday_type03,
            wday_type04,
            wday_type05,
            wplace_type
        FROM timetable_old
    )

    SELECT
        dates_cte.tab_num,
        dates_cte.date AS ymd_date,
        CASE
            -- когда график работы неделя через неделю
            -- проставляем null в выходные
            -- если разность между номерами недели текущей даты и начала - четные, работа в офисе
            -- если нечётные - из дома
            WHEN wplace_type = 3
            THEN CASE
                WHEN extract(dow FROM dates_cte.date) IN (0, 6) THEN NULL
                WHEN (extract(week FROM dates_cte.date) - extract(week FROM t_old_cte.start_date)) % 2 = 0 THEN 1
                ELSE 0
            END
            -- проставляем выходные null
            -- аналогично решению wplace_place = 3, но проверяем кратность 4
            -- если 0 или 1 - в офисе, остальные дома
            WHEN wplace_type = 4
            THEN CASE
                WHEN extract(dow FROM dates_cte.date) IN (0, 6) THEN NULL
                WHEN (extract(week FROM dates_cte.date) - extract(week FROM t_old_cte.start_date)) % 4 IN (0, 1) THEN 1
                ELSE 0
            END
            -- для каждого дня недели, где график различается по дням
            -- если день недели 1, берём wday_type01 и тд
            WHEN extract(dow FROM dates_cte.date) = 1 THEN t_old_cte.wday_type01
            WHEN extract(dow FROM dates_cte.date) = 2 THEN t_old_cte.wday_type02
            WHEN extract(dow FROM dates_cte.date) = 3 THEN t_old_cte.wday_type03
            WHEN extract(dow FROM dates_cte.date) = 4 THEN t_old_cte.wday_type04
            WHEN extract(dow FROM dates_cte.date) = 5 THEN t_old_cte.wday_type05
            ELSE NULL
        END AS to_be_at_office
    FROM dates_cte
    -- left join на старую таблицу по номеру работника
    -- дполнительное ограничение на версионность
    LEFT JOIN t_old_cte
        ON t_old_cte.tab_num = dates_cte.tab_num
        AND dates_cte.date BETWEEN t_old_cte.start_date AND t_old_cte.finish_date
    ORDER BY dates_cte.tab_num
    ) AS timetable_new;
    '''
    cursor.execute(transform_table)

    # завершение подключения
    conn.close()

    return 'ok'


def main():
    create()
    transform()


if __name__ == '__main__':
    main()