"""
Created on Mon Nov 11 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from sql.script_sql import dimensoes_questor, fato_omie
from datetime import datetime, timedelta
from etl import Questor_OMIE
import pandas as pd

cfg = Variable.get("questor_omie", deserialize_json=True)

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 11, 1),
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": cfg["emails_alerta"],
}

with DAG(
    "dag_questor_omie_v01",
    description="Processamento Questor para OMIE",
    default_args=default_args,
    schedule_interval="0 9 18 * *",
    tags=["conjel", "questor", "omie"],
    catchup=False,
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    dummy_staging = EmptyOperator(task_id="staging")
    dummy_dimensoes = EmptyOperator(task_id="dimensoes")
    fim = EmptyOperator(task_id="fim")

    @task
    def dados_questor(tabelas: str):
        print(f"Staging referente a tabela: {tabelas}")
        job = Questor_OMIE(
            schema="staging",
            conn_questor=cfg["conn_questor"],
            conn_datalake=cfg["conn_datalake"],
            table=tabelas,
        )
        job.datalake()

    task_staging_questor = dados_questor.expand(tabelas=cfg["tabelas"])

    with TaskGroup("criar_dimensoes") as task_dimensoes:
        dimensoes_questor()

    with TaskGroup("fato_omie") as task_fato_calculo_folha:
        fato_omie()

    @task
    def processamento_api(data_execucao, empresas: str):
        job = Questor_OMIE(conn_datalake=cfg["conn_datalake"])

        def write_pandas_excel(atualizado, falha):
            pd.DataFrame(atualizado).to_excel(
                r"/opt/airflow/dags/api_atualizado.xlsx", index=False
            )
            pd.DataFrame(falha).to_excel(
                r"/opt/airflow/dags/api_falha.xlsx", index=False
            )

        def arquivo_html(atualizado, falha):
            arquivo = (
                [
                    "/opt/airflow/dags/api_atualizado.xlsx",
                    "/opt/airflow/dags/api_falha.xlsx",
                ]
                if len(falha) > 0
                else ["/opt/airflow/dags/api_atualizado.xlsx"]
            )
            html_email = f"""
                        <html lang="pt-BR">
                        <head>
                            <title>Processamento OMIE</title>
                        </head>
                        <body>

                        <h1>Atualização contratos OMIE funcionários</h1>
                        <p>Olá, processamento realizado na data {data_execucao}.</p>
                        <p>Foram atualizados <b>{len(atualizado)}</b> {'folhas' if len(atualizado) > 1 else 'folha'}!</p>
                        <p>{'Não houve falha' if len(falha) == 0 else f'Houve falhas! Total de falhas: <b>{len(falha)}</b>'}.</p>
                        
                        </body>
                        </html>
                        """
            return arquivo, html_email

        cfg_omie_secrets = Variable.get(
            cfg[empresas]["variavel_secrets"], deserialize_json=True
        )
        app_key = cfg_omie_secrets["app_key"]
        app_secret = cfg_omie_secrets["app_secret"]
        codigo_servico = cfg[empresas]["codigo_servico"]
        omie_api = job.omie(
            data_competencia=data_execucao,
            url_contrato=cfg["url_contrato"],
            url_servico=cfg["url_servico"],
            app_key=app_key,
            app_secret=app_secret,
            codigo_servico=codigo_servico,
        )
        omie_atualizado = omie_api["atualizado"]
        omie_falha = omie_api["falha"]
        write_pandas_excel(omie_atualizado, omie_falha)
        arquivo_html(omie_atualizado, omie_falha)
        return {
            "to": cfg[empresas]["emails_processamento"],
            "files": arquivo_html[0],
            "html_content": arquivo_html[1],
        }

    task_processamento_api = processamento_api.partial(
        data_execucao="{{ next_ds }}"
    ).expand(empresas=cfg["empresas"])

    task_email = EmailOperator.partial(
        task_id="email", subject="Processamento folha OMIE"
    ).expand_kwargs(task_processamento_api)

    (
        inicio
        >> dummy_staging
        >> task_staging_questor
        >> dummy_dimensoes
        >> task_dimensoes
        >> task_fato_calculo_folha
        >> task_processamento_api
        >> task_email
        >> fim
    )
