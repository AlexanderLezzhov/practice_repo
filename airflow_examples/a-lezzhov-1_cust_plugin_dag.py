from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from a_lezzhov_1_ram_plugin import LezzhovCustomRAMOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

## ARGS
DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-lezzhov-1',
    'poke_interval': 10}

##DAG creation
with DAG('a-lezzhov-1_5_cust_plugin',
     schedule_interval='@daily',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-lezzhov-1']) as dag:

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение

## Functions definition

    def table_vals_check():
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM a_lezzhov_1_ram_location")
        records = cursor.fetchall()
        cursor.close()
        return len(records) == 0

## Tasks definition
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "a_lezzhov_1_ram_location"
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True
    )

    table_vals_parse = ShortCircuitOperator(
        task_id='table_vals_parse',
        python_callable=table_vals_check,
        dag=dag
    )

    locations_vals_parse = LezzhovCustomRAMOperator(
        task_id='locations_vals_parse'
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "INSERT INTO a_lezzhov_1_ram_location VALUES {{ ti.xcom_pull(task_ids='locations_vals_parse') }}",
        ],
        autocommit=True
    )

    create_table >> table_vals_parse >> locations_vals_parse >> load_data
