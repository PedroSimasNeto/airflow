"""
Created on Mon Jun 14 20:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.hooks.base import BaseHook
import psycopg2.extras as extras
import MySQLdb.cursors as cursors
import psycopg2
import MySQLdb 
import requests


def obter_conn_uri(database_id):
    conn = BaseHook.get_connection(database_id)
    return {
        "port": conn.port,
        "host": conn.host,
        "schema": conn.schema,
        "user": conn.login,
        "password": conn.password
    }


def airflow_buscar_conexao_postgres(database_id):
    """
            Retorna conexão com o postgres usando as configurações gravadas na metabase do airflow.

            Parâmetros
            - database_id (str) : Id da database gravada no airflow.

            Retorno
            - Conexao com o postgres.
            """
    config_db = obter_conn_uri(database_id)
    conn = psycopg2.connect(host=config_db["host"],
                            port=config_db["port"],
                            database=config_db["schema"],
                            user=config_db["user"],
                            password=config_db["password"])
    return conn


def airflow_buscar_conexao_mysql(database_id):
    """
            Retorna conexão com o postgres usando as configurações gravadas na metabase do airflow.

            Parâmetros
            - database_id (str) : Id da database gravada no airflow.

            Retorno
            - Conexao com o postgres.
            """
    config_db = obter_conn_uri(database_id)
    conn = MySQLdb.connect(host=config_db["host"],
                           port=config_db["port"],
                           database=config_db["schema"],
                           user=config_db["user"],
                           password=config_db["password"])
    return conn


def truncate_pgsql(database_id: str, table: str):
    """
        Trunca os dados da tabela no postgresql

        Parâmetros
        :param table: Tabela que será truncada
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_postgres(database_id) as pgsql_conn:
        with pgsql_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table};")
            pgsql_conn.commit()


def read_pgsql(database_id: str, query: str):
    """
        Obtém e resultado de uma consulta no postgresql

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_postgres(database_id) as pgsql_conn:
        with pgsql_conn.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(query, None)
            return cursor.fetchall()

def read_mysql(database_id: str, query: str):
    """
        Obtém e resultado de uma consulta no postgresql

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    with airflow_buscar_conexao_mysql(database_id) as mysql_conn:
        with mysql_conn.cursor(cursor_factory=cursors.DictCursor) as cursor:
            cursor.execute(query, None)
            return cursor.fetchall()


def delete_by_condition_pgsql(database_id, query: str):
    """
        Deleta dados de uma tabela no postgresql com ou sem condição

        Parâmetros
        :param query: Query a ser executada no banco
        :param database_id: Id da database gravada no Airflow
    """
    if not 'WHERE' in query:
        raise Exception("Are you trying to do a delete action without a condition? This can't be executed!")

    with airflow_buscar_conexao_postgres(database_id) as conn:
        with conn.cursor() as c:
            try:
                print(f'Executando query: "{query}"')
                c.execute(query, None)
                conn.commit()
            except Exception as ex:
                print(f'Excecao ao deletar dados no PostgreSQL: {str(ex)}')
                conn.rollback()
                raise ex


def get_api(url, headers):
    """
        Obtém dados via API através de GET

        Parâmetros
        :param url: URL da página que deverá retornar os dados
        :param headers: Dados de autorização para consultar API
    """
    response = requests.get(url, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise print("Falhou ao retornar API", e)
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
