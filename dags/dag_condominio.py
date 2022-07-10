"""
Created on Mon Jun 14 20:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow import DAG
from etl import Jobs

cfg_secrets = Variable.get("administradora_condominios_secret", deserialize_json=True)
cfg = Variable.get("administradora_condominios", deserialize_json=True)

default_args = {
    "owner": "pedro",
    "start_date": datetime(2022, 6, 14),
    "retry": 5,
    "retry_daily": timedelta(minutes=15)
}


@task
def st_condominios():
    parametro = Jobs(url=cfg["condominios"], header=cfg_secrets, database="postgres-datalake")
    parametro.st_importar_condominios(table="st_condominio")
    return print("Importado os condominios com sucesso!")


@task
def st_relatorio_receitas_despesas(data_execucao):
    parametro = Jobs(url=cfg["relatorios"], header=cfg_secrets, database="postgres-datalake")
    parametro.st_relatorio_receita_despesa(table="st_relatorio_receita_despesa", data_execucao=data_execucao,
                                           intervalo_execucao=cfg["intervalo_execucao"])
    return print("Importado os dados para staging com sucesso!")


with DAG(dag_id="dag_administradora_condominio", default_args=default_args,
         schedule_interval="30 2 * * 0", tags=["condominios"],
         catchup=False, max_active_runs=1
         ) as dag:

    inicio = DummyOperator(task_id="inicio")

    task_condominio = PostgresOperator(
        task_id="etl_condominio",
        postgres_conn_id="postgres-datalake",
        sql="""TRUNCATE TABLE CONDOMINIO;
                INSERT INTO CONDOMINIO(id_condominio, id_planoconta, nome_condominio, fantasia_condominio, cep_condominio, cpf_cnpj_condominio,
                                       endereco_condominio, complemento_condominio, bairro_condominio, cidade_condominio, uf_condominio)
                SELECT
                    cast(id_condominio_cond as int) as id_condominio, cast(id_planoconta_plc as int) as id_planoconta,
                    st_nome_cond as nome_condominio, st_fantasia_cond as fantasia_condominio,
                    cast(nullif(replace(st_cep_cond, '-', ''), '') as int) as cep_condominio,
                    cast(nullif(replace(replace(replace(st_cpf_cond, '-', ''), '/', ''), '.', ''), '') as bigint) as cpf_cnpj_condominio,
                    st_endereco_cond as endereco_condominio, st_complemento_cond as complemento_condominio,
                    st_bairro_cond as bairro_condominio, st_cidade_cond as cidade_condominio, st_uf_uf as uf_condominio
                FROM st_condominio;
            """
    )

    task_dimensao_conta_despesa = PostgresOperator(
        task_id="dimensao_conta_despesa",
        postgres_conn_id="postgres-datalake",
        sql="""TRUNCATE TABLE DIM_CONTA_DESPESA;
               INSERT INTO DIM_CONTA_DESPESA (CONTA, DESCRICAO, CONTA_NIVEL_1, CONTA_NIVEL_2)
                SELECT DISTINCT
                    conta, trim(descricao),
                    cast(nullif(split_part(conta, '.', 1), '') as int), cast(nullif(split_part(conta, '.', 2), '') as int)
                FROM ST_RELATORIO_RECEITA_DESPESA 
                where cast(nullif(split_part(conta, '.', 3), '') as int) is null 
                  and cast(nullif(split_part(conta, '.', 1), '') as int) = 2;
            """
    )

    data_fato = "{{ next_ds }}"

    task_fato_relatorio_despesa = PostgresOperator(
        task_id="fato_relatorio_despesa",
        postgres_conn_id="postgres-datalake",
        sql=[f"DELETE FROM FATO_RECEITA_DESPESA WHERE DATA BETWEEN cast('{data_fato}' as date) - interval '{cfg['intervalo_execucao']} Month' and '{data_fato}'",
             """INSERT INTO FATO_RECEITA_DESPESA(id_condominio, data, id_planoconta, id_conta,
                                                 conta_nivel_1, conta_nivel_2, conta_nivel_3, conta_nivel_4, conta_nivel_5, conta_nivel_6, 
                                                 descricao, valor)
                SELECT
                    id_condominio, cast(data as timestamp) as data, cast(idplanocontas as int) as id_planoconta, conta as id_conta,
                    cast(nullif(split_part(conta, '.', 1), '') as int), cast(nullif(split_part(conta, '.', 2), '') as int),
                    cast(nullif(split_part(conta, '.', 3), '') as int), cast(nullif(split_part(conta, '.', 4), '') as int),
                    cast(nullif(split_part(conta, '.', 5), '') as int), cast(nullif(split_part(conta, '.', 6), '') as int),
                    trim(descricao) as descricao, cast(valor as numeric) as valor
                FROM ST_RELATORIO_RECEITA_DESPESA;
            """]
    )

    fim = DummyOperator(task_id="fim")

    inicio >> st_condominios() >> task_condominio >> st_relatorio_receitas_despesas("{{ next_ds }}") >> [task_dimensao_conta_despesa, task_fato_relatorio_despesa] >> fim
