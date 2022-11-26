"""
Created on Mon Nov 11 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from sql.script_sql import dimensoes_questor
from datetime import datetime
from etl import Questor_OMIE
import pandas as pd

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 11, 1)
}

cfg = Variable.get("questor_omie", deserialize_json=True)
cfg_omie_secrets = Variable.get("api_omie_secrets", deserialize_json=True)


def dados_questor(tabelas):
    job = Questor_OMIE(schema="staging", conn_questor=cfg["conn_questor"], conn_datalake=cfg["conn_datalake"], table=tabelas)
    job.datalake()


def _processamento_api(**kwargs):
    job = Questor_OMIE(conn_datalake=cfg["conn_datalake"])
    omie_api = job.omie(data_competencia=kwargs["next_ds"], url_contrato=cfg["url_contrato"], url_servico=cfg["url_servico"], 
                        app_key=cfg_omie_secrets["app_key"], app_secret=cfg_omie_secrets["app_secret"], codigo_servico=cfg["codigo_servico"])
    print("Finalizado processamento da API!")    
    return omie_api


def _salvar_dados_api(**kwargs):
    atualizado = kwargs["ti"].xcom_pull(task_ids='processamento_api', key='atualizado')
    falha =  kwargs["ti"].xcom_pull(task_ids='processamento_api', key='falha')
    pd.DataFrame(atualizado).to_excel(r"/opt/airflow/dags/api_atualizado.xlsx", index=False)
    pd.DataFrame(falha).to_excel(r"/opt/airflow/dags/api_falha.xlsx", index=False)

    task_email = EmailOperator(
        task_id="email",
        to="pedros.itj@gmail.com",
        subject="Teste e-mail",
        html_content="Teste de e-mail",
        files=["/opt/airflow/dags/api_atualizado.xlsx", "/opt/airflow/dags/api_falha.xlsx"]
    )

    return task_email.execute(kwargs)


with DAG("dag_questor_omie_v01",
         description="Processamento Questor para OMIE",
         default_args=default_args, 
         schedule_interval=None,
         tags=["conjel", "questor", "omie"],
         catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")
    dummy_staging = DummyOperator(task_id="staging")
    dummy_dimensoes = DummyOperator(task_id="dimensoes")
    fim = DummyOperator(task_id="fim")

    with TaskGroup("criar_staging") as task_group_questor:
        task_questor = []
        for t in cfg["tabelas"]:
            task_questor.append(PythonOperator(
                task_id=f"questor_{t}",
                python_callable=dados_questor,
                op_kwargs={
                    "tabelas": t
                }
            ))

    with TaskGroup("criar_dimensoes") as task_dimensoes:
        dimensoes_questor()

    data_competencia = "{{ next_ds }}"

    task_fato_calculo_folha = PostgresOperator(
        task_id="fato_calculo_folha",
        postgres_conn_id="postgres-datalake",
        sql=[f"DELETE FROM CONJEL.FATO_CALCULO_FOLHA WHERE (DATA_PROCESSAMENTO = CURRENT_DATE OR data_inicial_folha = date_trunc('Month', cast('{data_competencia}' as date)) - interval '1 Month')",
            f"""
            INSERT INTO CONJEL.FATO_CALCULO_FOLHA
            SELECT
                current_date as data_processamento,
                coalesce(datainicialfolha, date_trunc('Month', cast('{data_competencia}' as date)) - interval '1 Month'),
                e.nomeempresa as empresa,
                substring(observacaosegmento from position('Contrato:' in observacaosegmento) +9 for position(',' in observacaosegmento) - 10) as contrato,
                substring(observacaosegmento from position('CNPJ:' in observacaosegmento) +5) as cnpj,
                case when count(distinct (c.codigofunccontr)) < 2 then 2
                    else count(distinct (c.codigofunccontr)) end as folhas_apuradas
            from CONJEL.QUESTOR_DIM_empresasegmento es
            inner join CONJEL.QUESTOR_DIM_empresa e on e.codigoempresa = es.codigoempresa
            left join CONJEL.QUESTOR_DIM_periodocalculo p on p.codigoempresa = e.codigoempresa
                                                         and p.datainicialfolha = date_trunc('Month', cast('{data_competencia}' as date)) - interval '1 Month'
            left join CONJEL.QUESTOR_DIM_funcpercalculo c on c.codigoempresa = p.codigoempresa
                                                         and c.codigopercalculo = p.codigopercalculo
            where es.CODIGOSEGMENTO in (19)
              and es.datafimsegmento is null
            group by 1,2,3,4,5
        """]
    )

    task_processamento_api = PythonOperator(
        task_id="processamento_api",
        python_callable=_processamento_api
    )

    task_email = PythonOperator(
        task_id="email",
        python_callable=_salvar_dados_api
    )

    inicio >> dummy_staging >> task_group_questor >> dummy_dimensoes >> task_dimensoes >> task_fato_calculo_folha >> task_processamento_api >> task_email >> fim
