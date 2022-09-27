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
from script_sql import dimensoes

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 9, 26)
}

cfg = Variable.get("cfg_conjel", deserialize_json=True)

def importa_stating(view: str):
    print(f"Importando staging {view}")
    job = Jobs_conjel(datalake=cfg["conn_datalake"])
    query = f"SELECT * FROM conjel.{view}"
    job.extract(conn_read=cfg["conn_tareffa"], query=query, table=n, schema="staging")

with DAG("dag_conjel_v01",
         description="DAG Conjel - Contabilidade",
         default_args=default_args, 
         schedule_interval=None,
         tags=["conjel", "contabilidade"],
         catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")

    staging_fim = DummyOperator(task_id="fim_staging")

    fim = DummyOperator(task_id="fim")

    task_staging = []

    with TaskGroup(group_id="staging") as task_group_staging:
        for n in cfg["views"]:
            task_staging.append(PythonOperator(
                task_id=n,
                python_callable=importa_stating,
                op_kwargs={
                    "view": n
                }
            ))

    with TaskGroup("dimensoes") as task_dimensoes:
        dimensoes()
    

    inicio >> task_staging >> staging_fim >> task_dimensoes >> fim