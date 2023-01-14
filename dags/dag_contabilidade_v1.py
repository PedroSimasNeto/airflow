"""
Created on Mon Sept 26 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from etl import Jobs_contabilidade
from sql.script_sql import dimensoes_tareffa

default_args = {"owner": "Pedro Simas", "start_date": datetime(2022, 9, 26)}

cfg = Variable.get("cfg_conjel", deserialize_json=True)


def importa_staging_tareffa(view: str):
    print(f"Importando staging {view}")
    job = Jobs_contabilidade(datalake=cfg["conn_datalake"])
    job.extract_data(
        conn_engine="postgresql",
        conn_read=cfg["conn_tareffa"],
        table=f"conjel.{view}",
        schema="staging",
    )


def importa_staging_qualyteam(table: str):
    print(f"Importando staging {table}")
    job = Jobs_contabilidade(datalake=cfg["conn_datalake"])
    job.extract_data(
        conn_engine="mysql+mysqldb",
        conn_read=cfg["conn_qualyteam"],
        table=table,
        schema="staging",
    )


def importa_staging_sankhya(table: str):
    print(f"Importando staging {table}")
    job = Jobs_contabilidade(datalake=cfg["conn_datalake"])
    job.extract_data(
        conn_engine="oracle+cx_oracle",
        conn_read=cfg["conn_sankhya"],
        table=table,
        schema="staging",
    )


with DAG(
    "dag_contabilidade_v1",
    description="DAG Contabilidade.",
    default_args=default_args,
    schedule_interval=None,
    tags=["contabilidade", "release_0.0.0"],
    catchup=False,
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    dummy_tareffa = EmptyOperator(task_id="tareffa")
    dummy_qualyteam = EmptyOperator(task_id="qualyteam")
    dummy_sankhya = EmptyOperator(task_id="sankhya")

    fim_staging_tareffa = EmptyOperator(task_id="fim_staging_tareffa")
    fim_staging_qualyteam = EmptyOperator(task_id="fim_staging_qualyteam")
    fim_staging_sankhya = EmptyOperator(task_id="fim_staging_sankhya")

    fim = EmptyOperator(task_id="fim")

    with TaskGroup(group_id="staging_tareffa") as task_staging_tareffa:
        task_staging = [
            PythonOperator(
                task_id=v,
                python_callable=importa_staging_tareffa,
                op_kwargs={"view": v},
            )
            for v in cfg["views_tareffa"]
        ]
    with TaskGroup("dimensoes_tareffa") as task_dimensoes_tareffa:
        dimensoes_tareffa()

    with TaskGroup("staging_qualyteam") as task_staging_qualyteam:
        task_staging = [
            PythonOperator(
                task_id=t,
                python_callable=importa_staging_qualyteam,
                op_kwargs={"table": t},
            )
            for t in cfg["tables_qualyteam"]
        ]
    with TaskGroup("staging_sankhya") as task_staging_sankhya:
        task_staging = [
            PythonOperator(
                task_id=t,
                python_callable=importa_staging_sankhya,
                op_kwargs={"table": t},
            )
            for t in cfg["tables_sankhya"]
        ]

    inicio >> [dummy_tareffa, dummy_qualyteam, dummy_sankhya]

    (
        dummy_tareffa
        >> task_staging_tareffa
        >> fim_staging_tareffa
        >> task_dimensoes_tareffa
        >> fim
    )

    dummy_qualyteam >> task_staging_qualyteam >> fim_staging_qualyteam >> fim
    dummy_sankhya >> task_staging_sankhya >> fim_staging_sankhya >> fim
