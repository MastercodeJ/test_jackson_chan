from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import json
import requests
from datetime import date

input_postgres_conn = "postgres_default"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0, 0, 0, 0),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def transform_data(data):
    transformed_list = []
    for item in data:
        tem_list = []
        for i in item.values():
            if type(i) == dict:
                my_string = json.dumps(i)
                tem_list.append(my_string)
            else:
                tem_list.append(i)
        transformed_list.append(tuple(tem_list))
    return transformed_list


def check_if_existing():
    pg_hook = PostgresHook(
        postgres_conn_id=input_postgres_conn,
    )
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'forecast')"
    )
    exists = cursor.fetchall()[0]
    conn.close()
    cursor.close()
    return exists[0]


def create_table():
    pg_hook = PostgresHook(
        postgres_conn_id=input_postgres_conn,
    )
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE forecast (
        forecast_date DATE,
        week VARCHAR(10),
        forecast_wind VARCHAR(50),
        forecast_weather VARCHAR(100),
        forecast_maxtemp JSONB,
        forecast_mintemp JSONB,
        forecast_maxrh JSONB,
        forecast_minrh JSONB,
        Forecast_icon INTEGER,
        psr VARCHAR(10),
        insert_date DATE DEFAULT CURRENT_DATE,
        PRIMARY KEY (forecast_date, insert_date)
    );"""
    )
    conn.commit()
    conn.close()
    cursor.close()


def insert_data(data):
    pg_hook = PostgresHook(
        postgres_conn_id=input_postgres_conn,
    )
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    today = date.today()

    for i in data:
        select_statement = "SELECT COUNT(*) FROM forecast WHERE forecast_date = %(forecast_date)s AND insert_date = %(today)s"
        cursor.execute(select_statement, {"forecast_date": i[0], "today": today})
        result = cursor.fetchone()[0]
        if result == 0:
            insert_statement = "INSERT INTO forecast (forecast_date, week, forecast_wind, forecast_weather, forecast_maxtemp, forecast_mintemp, forecast_maxrh, forecast_minrh, forecast_icon, psr) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_statement, i)
            conn.commit()
            print("Data inserted successfully.")
        else:
            print("Data already exists. No insert performed.")
    conn.close()
    cursor.close()


def main():
    url = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=tc"
    response = requests.get(url)
    if response.status_code == 200:
        # Process the response data
        data = response.json()["weatherForecast"]
        transform_list = transform_data(data)
        if check_if_existing():
            insert_data(transform_list)
            print("Data inserted into existing table.")
        else:
            create_table()
            insert_data(transform_list)
            print("Table created and data inserted.")
    else:
        # Handle the error
        print("Error: Failed to fetch data. Status code:", response.status_code)


main()

with DAG(
    "hko_weather_forecast",
    default_args=default_args,
    description="hko_weather_forecast",
    catchup=False,
    schedule_interval="0 8 * * *",
) as dag:
    task_1 = PythonOperator(
        task_id="fetch_data1",
        python_callable=main,
        dag=dag,
    )
    task_1
