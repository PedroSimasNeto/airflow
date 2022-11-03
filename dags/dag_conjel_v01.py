"""
Created on Mon Sept 26 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from etl import Jobs_conjel
from script_sql import dimensoes_tareffa

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 9, 26)
}

cfg = Variable.get("cfg_conjel", deserialize_json=True)


def importa_stating_tareffa(view: str):
    print(f"Importando staging {view}")
    job = Jobs_conjel(datalake=cfg["conn_datalake"])
    query = f"SELECT * FROM conjel.{view}"
    job.extract(conn_type="postgres", conn_read=cfg["conn_tareffa"], query=query, table=view, schema="staging")


def importa_stating_qualyteam(table: str):
    print(f"Importando staging {table}")
    job = Jobs_conjel(datalake=cfg["conn_datalake"])
    query = f"SELECT * FROM {table}"
    job.extract(conn_type="mysql", conn_read=cfg["conn_qualyteam"], query=query, table=table, schema="staging")


with DAG("dag_conjel_v01",
         description="DAG Conjel - Contabilidade.",
         default_args=default_args, 
         schedule_interval=None,
         tags=["conjel", "contabilidade"],
         catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")

    dummy_tareffa = DummyOperator(task_id="tareffa")
    dummy_qualyteam = DummyOperator(task_id="qualyteam")

    fim_staging_tareffa = DummyOperator(task_id="fim_staging_tareffa")
    fim_staging_qualyteam = DummyOperator(task_id="fim_staging_qualyteam")

    fim = DummyOperator(task_id="fim")

    with TaskGroup(group_id="staging_tareffa") as task_staging_tareffa:
        task_staging = []
        for v in cfg["views_tareffa"]:
            task_staging.append(PythonOperator(
                task_id=v,
                python_callable=importa_stating_tareffa,
                op_kwargs={
                    "view": v
                }
            ))

    with TaskGroup("dimensoes_tareffa") as task_dimensoes_tareffa:
        dimensoes_tareffa()

    with TaskGroup("staging_qualyteam") as task_staging_qualyteam:
        task_staging = []
        for t in cfg["tables_qualyteam"]:
            task_staging.append(PythonOperator(
                task_id=t,
                python_callable=importa_stating_qualyteam,
                op_kwargs={
                    "table": t
                }
            ))
    

    inicio >> [dummy_tareffa, dummy_qualyteam]
    dummy_tareffa >> task_staging_tareffa >> fim_staging_tareffa >> task_dimensoes_tareffa >> fim
    dummy_qualyteam >> task_staging_qualyteam  >> fim_staging_qualyteam >> fim