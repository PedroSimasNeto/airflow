"""
Created on Mon Nov 11 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
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


def dados_questor(tabelas):
    job = Questor_OMIE(schema="staging", conn_questor=cfg["conn_questor"], conn_datalake=cfg["conn_datalake"], table=tabelas)
    job.datalake()


with DAG("dag_questor_omie_v01",
         description="Processamento Questor para OMIE",
         default_args=default_args, 
         schedule_interval=None,
         tags=["conjel", "questor", "omie"],
         catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    with TaskGroup("staging") as task_group_questor:
        task_questor = []
        for t in cfg["tabelas"]:
            task_questor.append(PythonOperator(
                task_id=f"questor_{t}",
                python_callable=dados_questor,
                op_kwargs={
                    "tabelas": t
                }
            ))

    with TaskGroup("dimensoes") as task_dimensoes:
        dimensoes_questor()

    inicio >> task_group_questor >> task_dimensoes >> fim
