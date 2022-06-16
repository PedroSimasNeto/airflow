from airflow.hooks.base import BaseHook
import psycopg2
import psycopg2.extras as extras


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
        with pgsql_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, None)
            return cursor.fetchall()
