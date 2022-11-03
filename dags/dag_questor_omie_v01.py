"""
Created on Mon Nov 11 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
from etl import Questor_OMIE

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 11, 1)
}

cfg = Variable.get("questor_omie", deserialize_json=True)


def dados_questor(tabelas):
    job = Questor_OMIE(schema="staging", conn="questor", table=tabelas)
    job.datalake()


with DAG("dag_questor_omie_v01",
         description="Processamento Questor para OMIE",
         default_args=default_args, 
         schedule_interval=None,
         tags=["conjel", "questor", "omie"],
         catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    task_questor = []

    for t in cfg["tabelas"]:
        task_questor.append(PythonOperator(
            task_id=f"questor_{t}",
            python_callable=dados_questor,
            op_kwargs={
                "tabelas": f
            }
        ))

    inicio >> task_questor >> fim
