"""
Created on Mon Nov 11 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from sql.script_sql import dimensoes_questor
from datetime import datetime
from etl import Questor_OMIE

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
    omie_api = job.omie(data_competencia=kwargs["next_ds"], url_contrato=cfg["url_contrato"], url_cliente=cfg["url_cliente"], 
                        app_key=cfg_omie_secrets["app_key"], app_secret=cfg_omie_secrets["app_secret"], codigo_servico=cfg["codigo_servico"])
    print("Finalizado processamento da API!")    
    return omie_api


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

    task_processamento_api = PythonOperator(
        task_id="processamento_api",
        python_callable=_processamento_api
    )

    task_fato_calculo_folha = PostgresOperator(
        task_id="fato_calculo_folha",
        postgres_conn_id="postgres-datalake",
        sql=["DELETE FROM CONJEL.FATO_CALCULO_FOLHA WHERE DATA_PROCESSAMENTO = CURRENT_DATE",
            """
            INSERT INTO CONJEL.FATO_CALCULO_FOLHA
            SELECT
                current_date as data_processamento,
                datainicialfolha,
                e.nomeempresa as empresa,
                substring(observacaosegmento from position('Contrato:' in observacaosegmento) +9 for 10) as contrato,
                substring(observacaosegmento from position('CNPJ:' in observacaosegmento) +5) as cnpj,
                count(distinct (c.codigofunccontr)) as folhas_apuradas
            from CONJEL.QUESTOR_DIM_funcpercalculo c
            inner join CONJEL.QUESTOR_DIM_periodocalculo p on p.codigoempresa = c.codigoempresa
                                                          and p.codigopercalculo = c.codigopercalculo
            inner join (select codigoempresa as codemp,
                            codigofunccontr as codfunc,
                            max (datatransf) as datafunc
                        from CONJEL.QUESTOR_DIM_funclocal
                        group by 1,2) h on h.codemp = c.codigoempresa
                                       and h.codfunc = c.codigofunccontr
            inner join CONJEL.QUESTOR_DIM_funclocal l on l.codigoempresa = c.codigoempresa
                                                     and l.codigofunccontr = c.codigofunccontr
                                                     and l.datatransf = h.datafunc    
            inner join CONJEL.QUESTOR_DIM_empresa e on e.codigoempresa = c.codigoempresa
            inner join CONJEL.QUESTOR_DIM_usuario u on u.codigousuario = c.codigousuario
            inner join CONJEL.QUESTOR_DIM_empresasegmento es on es.codigoempresa = c.codigoempresa 
                                                            and es.CODIGOSEGMENTO in (19)
                                                            and es.datafimsegmento is null
            where p.datainicialfolha = date_trunc('Month', cast('{{ params.data_competencia }}' as date)) - interval '1 Month'
            group by 1,2,3,4,5
        """],
        parameters={"data_competencia": "{{ next_ds }}"}
    )

    inicio >> dummy_staging >> task_group_questor >> dummy_dimensoes >> task_dimensoes >> task_fato_calculo_folha >> task_processamento_api >> fim
