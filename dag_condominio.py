from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow import DAG
from etl import jobs

cfg_secrets = Variable.get("administradora_condominios_secret", deserialize_json=True)
cfg = Variable.get("administradora_condominios", deserialize_json=True)

default_args = {
    "owner": "pedro",
    "start_date": datetime(2022, 6, 14),
    "retry": 5,
    "retry_daily": timedelta(minutes=15)
}


@task
def condominios():
    parametro = jobs(url=cfg["condominios"], header=cfg_secrets, database="postgres-datalake")
    parametro.importar_condominios(table="condominio")
    return print("Importado os condominios com sucesso!")


@task
def relatorio_receitas_despesas(data_execucao):
    parametro = jobs(url=cfg["relatorios"], header=cfg_secrets, database="postgres-datalake")
    parametro.relatorio_receita_despesa(table="relatorio_receita_despesa", data_execucao=data_execucao,
                                        intervalo_execucao=cfg["intervalo_execucao"])
    return print("Importado os dados do relatório de receitas e despesas com sucesso!")


with DAG(dag_id="dag_administradora_condominio", default_args=default_args,
         schedule_interval="30 2 * * 0", tags=["condominios"]
         ) as dag:

    inicio = DummyOperator(task_id="inicio")

    fim = DummyOperator(task_id="fim")

    inicio >> condominios() >> relatorio_receitas_despesas("{{ next_ds }}") >> fim
