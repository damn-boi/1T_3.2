from time import localtime, strftime
from datetime import datetime
import pandas as pd
import requests
import decimal
import psycopg2

try:
    conn = psycopg2.connect(database = "postgres", user = "postgres", 
                            password = "123", host = "localhost", port = "5434")
except:
    print("I am unable to connect to the database") 

url_conf = {
            "table_name": "rates",
            "rate_base": "BTC",
            "rate_target": "RUB",
            "url_base": "https://api.exchangerate.host/"
}

hist_date = pd.date_range('2023-06-01','2023-06-30', freq='D').strftime("%Y-%m-%d").tolist()

cur = conn.cursor()

try:
    cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, date date, first_curr text, second_curr text, exchange_rate decimal(10, 3));")
except Exception as err:
    print(f'Error occured: {err}')

month_course = []
for i in range(len(hist_date)):
    url = url_conf['url_base'] + hist_date[i]
    try:
        response = requests.get(url, params = {'base': url_conf['rate_base']})
    except Exception as err:
        print(f'Error occured at response: {err}')
    data = response.json()
    # print(data)
    value = str(decimal.Decimal(data['rates']['RUB']))[:10]
    
    date1 = hist_date[i]
    try:
        data = [(i, date1, 'BTC', 'RUB', value)]
        sql = "INSERT INTO test VALUES(%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;"
        cur.executemany(sql, data)
    except Exception as err:
        print(f'Error occured at insert: {err}')

try:
    query = "SELECT * FROM test"
    cur.execute(query)
    results = cur.fetchall()
    max = -1
    min = 999999999999
    sum = 0
    last_course_element = 0
    for row in results:
        if row[4] > max: max = row[4]
        if row[4] < min: min = row[4]
        sum += row[4]
        if hist_date[-1] == '2023-06-30':
            last_course_element = row[4]
    print('максимальное значение курса: ' + str(max))
    print('минимальное значение курса: ' + str(min))
    print('среднее значение курса: ' + str(sum/len(hist_date)))
    print('значение курса на последний день месяца: ' + str(last_course_element))
    minday = " "
    maxday = " "
    for row in results:
        if max == row[4]:
            print('день, в который значение курса было максимальным: ' + str(row[1]))
            maxday = row[1]
        if min == row[4]:
            print('день, в который значение курса было минимальным: ' + str(row[1]))
            minday = row[1]
    
except Exception as err:
    print(f'Error occured at query execution: {err}')

try:
    cur.execute("CREATE TABLE IF NOT EXISTS test2 (id serial PRIMARY KEY, month varchar(8), first_curr text, " 
                "second_curr text, maxday_course date, minday_course date, max_course decimal(10, 3), "
                "min_course decimal(10, 3), aver_course decimal(10, 3), month_rate_last decimal(10, 3));")
except Exception as err:
    print(f'Error occured at creating test2: {err}')

try:
    data = [(1, '2023-06', 'BTC', 'RUB', maxday, minday, max, min, sum/len(hist_date), last_course_element)]
    sql = "INSERT INTO test2 VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;"
    cur.executemany(sql, data)
except Exception as err:
    print(f'Error occured at insert test2: {err}')
          
conn.commit()
conn.close()
cur.close()