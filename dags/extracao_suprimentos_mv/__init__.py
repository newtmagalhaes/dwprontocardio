import os
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy as sa
from airflow.decorators import dag, task

from dags.extracao_suprimentos_mv import hooks
from dags.extracao_suprimentos_mv.criar_tabela_raw import criar_tabela_raw

engine_postgres = sa.create_engine(hooks.postgres_hook.get_uri())
nome_schema_postgres = 'raw_mv'


def gerar_texto_querys(query: str, maior_id: int):

    sql_dir = os.path.join(os.path.dirname(__file__), 'raw_mv', 'modelos')
    sql_path = os.path.join(sql_dir, f'{query}.sql')

    with open(sql_path, 'r') as arq:
        texto_query = arq.read()

        if ':MAIOR_ID' not in texto_query:
            texto_query
            
        else:
            if maior_id == 0:
                maior_id = 1
                texto_query = texto_query.replace(':MAIOR_ID', str(maior_id))
            else:
                texto_query = texto_query.replace(':MAIOR_ID', str(maior_id))

        print(texto_query)
        return texto_query


def inserir_dados_postgres(tabela:str, colunas:str, place_insert:str, df):

    print(colunas)
    print(place_insert)

    with postgres_hook.get_conn() as conn_pg:
        cursor_pg = conn_pg.cursor()

        query = f"INSERT INTO {nome_schema_postgres}.{tabela} ({colunas}) VALUES ({place_insert})"
        registros = [tuple(row) for row in df.to_numpy()]
        cursor_pg.executemany(query, registros)

        conn_pg.commit()



def apagar_tabela(tabela: str, conn):
    
    query = f"TRUNCATE TABLE {nome_schema_postgres}.{tabela} CASCADE"
    conn.execute(query)

    # query_max_id = f"SELECT * FROM {nome_schema_postgres}.{tabela}"
    # conn.execute(query_max_id)
    # resultado = conn.fetchall()
    # print(tabela)
    # print(resultado)




default_args = {
    'owner' : 'airflow_suprimentos',
    'start_date' : datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
}

@dag(
    dag_id= "Extracao_Suprimentos_MV",
    default_args=default_args,
    description= "Extracao de Dados da Produção p/ Camada Raw - SUPRIMENTOS",
    schedule_interval=timedelta(minutes=20),
    catchup=False,
)
def extracao_oracle_for_postgres():
    

    for nome_tabela in TABELAS_POSTGRES:

        @task(task_id=f"obter_maior_id_{nome_tabela}")
        def obter_maior_id(nome_tabela: str):

            with postgres_hook.get_conn() as conn_pg:
                cursor_pg = conn_pg.cursor()

                inspector = sa.inspect(engine_postgres)
                bool_table = inspector.has_table(nome_tabela, nome_schema_postgres)

                if bool_table:

                    apagar_tabela(nome_tabela, cursor_pg) if nome_tabela in ['produto', 'est_pro'] else None

                    query_max_id = f"SELECT MAX(id_{nome_tabela}) FROM {nome_schema_postgres}.{nome_tabela}"
                    cursor_pg.execute(query_max_id)
                    resultado = cursor_pg.fetchone()[0]

                    # print(f"Function 'obter_maior_id_{nome_tabela}', {resultado}")

                    maior_id = resultado if resultado is not None else 0

                else:
                    
                    criar_tabela_raw(nome_tabela, cursor_pg)
                    maior_id = 0

                cursor_pg.close()
                return maior_id


        @task(task_id=f"extrair_dados_oracle_{nome_tabela}")
        def extrair_dados_oracle(nome_tabela: str, maior_id: int):

            with oracle_hook.get_conn() as conn_ora:
                cursor_ora = conn_ora.cursor()

                texto_query = gerar_texto_querys(nome_tabela, maior_id)

                cursor_ora.execute(texto_query)
                metadados_query = cursor_ora.description

                colunas = [registro[0] for registro in metadados_query]
                nome_colunas = ', '.join(f'"{col}"'for col in colunas)

                placeholders_insert = ', '.join(['%s'] * len(colunas))

                cursor_ora.execute(texto_query)
                records = cursor_ora.fetchall()

                df = pd.DataFrame(records, columns=None)
                df = df.replace({pd.NaT: None})

                inserir_dados_postgres(nome_tabela, nome_colunas, placeholders_insert, df)

        
        maior_id = obter_maior_id(nome_tabela)
        extrair_dados_oracle(nome_tabela, maior_id)


extracao_oracle_for_postgres()