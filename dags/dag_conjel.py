from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
from etl import Jobs_conjel

default_args = {
    "owner": "Pedro Simas",
    "start_date": datetime(2022, 9, 26, 0, 0, 0)
}

cfg = Variable.get("cfg_conjel", deserialize_json=True)

with DAG("dag_conjel_v01", default_args=default_args, schedule_interval=None, catchup=False) as dag:

    inicio = DummyOperator(task_id="inicio")

    fim = DummyOperator(task_id="fim")

    for n in cfg["views"]:
        @task(task_id=n)
        def importa_stating():
            job = Jobs_conjel(datalake=cfg["conn_datalake"])
            query = f"SELECT * FROM conjel.{n}"
            job.extract(conn_read=cfg["tareffa"], query=query, table=n, schema="staging")
        inicio >> importa_stating() >> fim