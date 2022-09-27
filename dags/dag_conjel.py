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

    task_staging = []

    for n in cfg["views"]:
        task_staging.append(PythonOperator(
            task_id=n,
            python_callable=importa_stating,
            op_kwargs={
                "view": n
            }
        ))

    task_dimensao_empresas = PostgresOperator(
        task_id="dimensao_empresas",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas;",
            """INSERT INTO conjel.tareffa_dim_empresas (id_empresa, codigo_questor, razao_social, cnpj, id_cnae, inicio_prestacao_servicos, matriz, inscricao_estadual, status_ativa, id_regime)
                SELECT
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
                FROM staging.view_empresas ve;
                    """],
        autocommit=True
    )

    task_dimensao_regime = PostgresOperator(
        task_id="dimensao_regime",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_regime;",
            """INSERT INTO conjel.tareffa_dim_regime (id, descricao)
                SELECT distinct idregime as id, regimedescricao as descricao 
                FROM staging.view_empresas ve;
            """],
        autocommit=True
    )

    task_dimensao_empresas_caracteristicas = PostgresOperator(
        task_id="dimensao_empresas_caracteristicas",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas_caracteristicas;",
            """INSERT INTO conjel.tareffa_dim_empresas_caracteristicas (id_empresa, id_caracteristica, id_grupo_caracteristica)
                SELECT
                    idempresa as id_empresa,
                    idcaracteristica as id_caracteristica,
                    cast(split_part(grupocaracteristica, '.', 1) as smallint) as id_grupo_caracteristica
                FROM staging.view_empresas_caracteristicas vec;
            """],
        autocommit=True
    )
    
    task_dimensao_grupo_caracteristica = PostgresOperator(
        task_id="dimensao_grupo_caracteristica",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_grupo_caracteristica;",
            """INSERT INTO conjel.tareffa_dim_grupo_caracteristica (id, id_empresa, descricao)
                SELECT distinct
                    cast(split_part(grupocaracteristica, '.', 1) as smallint) as id,
                    idempresa as id_empresa, 
                    trim(substring(grupocaracteristica FROM position('.' in grupocaracteristica) + 1 for length(grupocaracteristica))) as descricao
                FROM staging.view_empresas_caracteristicas vec;
            """],
        autocommit=True
    )
    
    task_dimensao_caracteristica = PostgresOperator(
        task_id="dimensao_caracteristica",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_caracteristica;",
            """INSERT INTO conjel.tareffa_dim_caracteristica (id, descricao)
                SELECT distinct idcaracteristica as id, caracteristica as descricao
                FROM staging.view_empresas_caracteristicas vec;
            """],
        autocommit=True
    )
    
    task_dimensao_departamento = PostgresOperator(
        task_id="dimensao_departamento",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_departamento;",
            """INSERT INTO conjel.tareffa_dim_departamento (id, descricao)
                with depart as (
                    SELECT distinct departamento as descricao
                    FROM staging.view_empresas_responsaveis ver)
                SELECT row_number() over (order by descricao) as id, descricao FROM depart;
            """],
        autocommit=True
    )
    
    task_dimensao_empresas_servicos = PostgresOperator(
        task_id="dimensao_empresas_servicos",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas_servicos;",
            """INSERT INTO conjel.tareffa_dim_empresas_servicos (id_empresa, id_servico, ativo, vencimento_alternativo)
                SELECT
                    idempresa as id_empresa,
                    idservico as id_servico,
                    case when ativo is true then 1
                         else 0 end as ativo,
                    vencimento_alternativo
                FROM staging.view_empresas_servicos ves;
            """],
        autocommit=True
    )
    
    task_dimensao_servicos = PostgresOperator(
        task_id="dimensao_servicos",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_servicos;",
            """INSERT INTO conjel.tareffa_dim_servicos (id, descricao)
                SELECT distinct idservico as id, servico as descricao
                FROM staging.view_empresas_servicos ves;
            """],
        autocommit=True
    )

    inicio >> task_staging >> staging_fim >> [task_dimensao_empresas, task_dimensao_regime, task_dimensao_empresas_caracteristicas, 
                                              task_dimensao_grupo_caracteristica, task_dimensao_caracteristica, task_dimensao_departamento,
                                              task_dimensao_empresas_servicos, task_dimensao_servicos] >> fim