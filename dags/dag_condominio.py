"""
Created on Mon Jun 14 20:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.settings import TIMEZONE
from airflow import DAG
from etl import jobs
import pendulum

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
    parametro = jobs(url=cfg["condominios"], header=cfg_secrets, database="postgres-datalake")
    parametro.st_importar_condominios(table="st_condominio")
    return print("Importado os condominios com sucesso!")


@task
def st_relatorio_receitas_despesas(data_execucao):
    parametro = jobs(url=cfg["relatorios"], header=cfg_secrets, database="postgres-datalake")
    parametro.st_relatorio_receita_despesa(table="st_relatorio_receita_despesa", data_execucao=data_execucao,
                                           intervalo_execucao=cfg["intervalo_execucao"])
    return print("Importado os dados para staging com sucesso!")


with DAG(dag_id="dag_administradora_condominio", default_args=default_args,
         schedule_interval="30 2 * * 0", tags=["condominios"],
         catchup=False, max_active_runs=1
         ) as dag:

    inicio = DummyOperator(task_id="inicio")

    task_dimensao_conta_despesa = PostgresOperator(
        task_id="dimensao_conta_despesa",
        postgres_conn_id="postgres-datalake",
        sql="""TRUNCATE TABLE CONTAS_DESPESAS;
               INSERT INTO CONTAS_DESPESAS (CONTA, DESCRICAO, CONTA_NIVEL_1, CONTA_NIVEL_2)
                SELECT DISTINCT
                    conta, trim(descricao),
                    cast(nullif(split_part(conta, '.', 1), '') as int), cast(nullif(split_part(conta, '.', 2), '') as int) as conta_nivel_2
                from relatorio_receita_despesa 
                where cast(nullif(split_part(conta, '.', 3), '') as int) is null 
                  and cast(nullif(split_part(conta, '.', 1), '') as int) = 2;
            """
    )

    fim = DummyOperator(task_id="fim")

    inicio >> st_condominios() >> st_relatorio_receitas_despesas("{{ ds }}") >> [task_dimensao_conta_despesa] >> fim
