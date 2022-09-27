"""
Created on Mon Sept 26 19:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from etl import Jobs_conjel

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

    for n in cfg["views"]:
        task_staging = PythonOperator(
            task_id=n,
            python_callable=importa_stating(view=n)
        )

    task_dimensao_empresas = PostgresOperator(
        task_id="dimensao_empresas",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas;",
            """insert into conjel.tareffa_dim_empresas (id_empresa, codigo_questor, razao_social, cnpj, id_cnae, inicio_prestacao_servicos, matriz, inscricao_estadual, status_ativa, id_regime)
                select
                    idempresa as id_empresa,
                    codigoquestor as codigo_questor,
                    razaosocial as razao_social,
                    cnpj,
                    idcnae as id_cnae,
                    cast(inicioprestacaoservicos as date) as inicio_prestacao_servicos,
                    case when ematriz is true then 1
                        else 0 end as matriz,
                    inscricaoestadual as inscricao_estadual,
                    case when statusativa is true then 1
                        else 0 end status_ativa,
                    idregime as id_regime
                    from staging.view_empresas ve;
                    """],
        autocommit=True
    )

    inicio >> task_staging >> staging_fim >> task_dimensao_empresas >> fim