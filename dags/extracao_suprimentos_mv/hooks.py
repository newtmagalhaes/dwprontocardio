from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

postgres_hook = PostgresHook(postgres_conn_id='postgres_prontocardio')
oracle_hook = OracleHook(oracle_conn_id='oracle_prontocardio', thick_mode=True)
