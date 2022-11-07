"""
Created on Mon Jun 14 20:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import psycopg2.extras as extras
import MySQLdb.cursors as cursors
import firebirdsql
import requests
import json


def obter_conn_uri(database_id):
    conn = BaseHook.get_connection(database_id)
    return {
        "port": conn.port,
        "host": conn.host,
        "schema": conn.schema,
        "user": conn.login,
        "password": conn.password,
        "extra": conn.extra
    }


def airflow_buscar_conexao_firebird(database_id):
    """
            Retorna conexão com o postgres usando as configurações gravadas na metabase do airflow.

            Parâmetros
            - database_id (str) : Id da database gravada no airflow.

            Retorno
            - Conexao com o Firebird.
            """
    config_db = obter_conn_uri(database_id)
    conn = firebirdsql.connect(host=config_db["host"],
                               port=config_db["port"],
                               database=config_db["schema"],
                               user=config_db["user"],
                               password=config_db["password"],
                               charset=json.loads(config_db['extra'])['charset'])
    return conn


def read_pgsql(database_id: str, query: str):
    """
        Obtém o resultado de uma consulta no PostgreSQL

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    postgres_hook = PostgresHook(postgres_conn_id=database_id)
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query=query)
            result = cursor.fetchall()
            return result


def read_mysql(database_id: str, query: str):
    """
        Obtém o resultado de uma consulta no MySQL

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    mysql_hook = MySqlHook(mysql_conn_id=database_id)
    get_conn = mysql_hook.get_conn()
    result = get_conn._execute_query(query=query)
    return result


def read_firebird(database_id: str, query: str):
    """
        Obtém o resultado de uma consulta no Firebird

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_firebird(database_id) as firebird_conn:
        with firebird_conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


def read_oracle(database_id: str, query: str):
    """
        Obtém o resultado de uma consulta no Oracle
        
        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    oracle_hook = OracleHook(oracle_conn_id=database_id)
    result = oracle_hook.get_records(sql=query)
    return result


def truncate_pgsql(database_id: str, table: str):
    """
        Trunca os dados da tabela no postgresql

        Parâmetros
        :param table: Tabela que será truncada
        :param database_id: Id da database gravada no Airflow
    """
    postgres_hook = PostgresHook(database_id)
    postgres_hook.run(sql=f"TRUNCATE TABLE {table};", autocommit=True)


def delete_by_condition_pgsql(database_id, query: str):
    """
        Deleta dados de uma tabela no postgresql com ou sem condição

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    if not 'WHERE' in query:
        raise Exception("Are you trying to do a delete action without a condition? This can't be executed!")

    postgres_hook = PostgresHook(database_id)
    postgres_hook.run(sql=query, autocommit=True)


def api(method: str, url: str, headers: dict, json=None, **kwargs):
    """
        Obtém dados via API através de GET

        Parâmetros
        :param url: URL da página que deverá retornar os dados
        :param headers: Dados de autorização para consultar API
    """
    if method.upper() == "GET":
        response = requests.get(url, headers=headers, json=json, **kwargs)
    if method.upper() == "POST":
        response = requests.post(url, headers=headers, json=json, **kwargs)
    # try:
    #     response.raise_for_status()
    # except requests.exceptions.HTTPError as e:
    #     raise print("Falhou ao retornar API", e)
    return response



def task_failure_alert(context):
    failed_alert = TelegramOperator(
        task_id="telegram_failed",
        text=f"""
            Task Failed.
            <b>Task</b>: {context.get('task_instance').task_id}
            <b>Dag</b>: {context.get('task_instance').dag_id}
            <b>Execution Time</b>: {context.get('execution_date').strftime("%Y-%m-%d %H:%M")}
            <b>Log URL</b>: {context.get('task_instance').log_url}
            """,
        chat_id="-1001619323454"
    )
    return failed_alert.execute(context)
