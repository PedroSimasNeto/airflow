"""
Created on Mon Jun 14 20:00:00 2022

@author: Pedro Simas Neto
"""
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import utils as ut
import math


class Jobs_condominios:
    def __init__(self, url, header, database):
        self.url_job = url
        self.header_job = header
        self.database_job = database

    def st_importar_condominios(self, table: str, schema: str):
        response = ut.api(method="GET", url=self.url_job, headers=self.header_job)

        # Transformado o retorno da API em Dataframe Pandas.
        df_condominio = pd.DataFrame(response.json())

        # Obtendo a conexão cadastrada do PostgreSQL (Datalake) no Airflow.
        connection = ut.obter_conn_uri(self.database_job)

        print(f"Inserindo dados na tabela {table}")
        engine = create_engine(
            f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}'
        )
        df_condominio.to_sql(
            table, engine, schema=schema, if_exists="replace", index=False
        )

    def st_relatorio_receita_despesa(
        self, table: str, data_execucao: str, intervalo_execucao: int, schema: str
    ):
        # Obtendo a data de execução do Scheduler e diminuindo pelos numeros de meses parametrizados no Airflow.

        # Transformando a string em data
        dt_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
        # Variável que armazena a última data de processamento e o último condomínio processado
        variable_processamento = Variable.get(
            "condominios_atualizacao", deserialize_json=True
        )
        data_ult_processamento = datetime.strptime(
            variable_processamento["schedule_dag"], "%Y-%m-%d"
        )
        condominio_ult_processamento = variable_processamento["id_condominio"]
        # Dimunuindo os números de meses para reprocessamento.
        data_inicio = dt_execucao.replace(day=1) - relativedelta(
            months=intervalo_execucao
        )
        data_fim = dt_execucao
        # Criando range de datas para o laço de repetição.
        data = pd.date_range(data_inicio, data_fim, freq="D")
        print(
            f"Reprocessando entre os dias {data_inicio.strftime('%Y-%m-%d')} a {data_fim.strftime('%Y-%m-%d')}"
        )

        # Query que busca no banco de dados os condomínios cadastrados para buscar na API.
        dado_condominio = ut.read_pgsql(
            self.database_job,
            "select array_agg(distinct id_condominio) from dim_condominio;",
        )[0][0]

        # Obtendo a conexão cadastrada do PostgreSQL (Datalake) no Airflow.
        connection = ut.obter_conn_uri(self.database_job)
        engine = create_engine(
            f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}'
        )

        def _processamento_condominios(condominios):
            try:
                for d2 in condominios:
                    # Criado lista que será preenchida com os dados da API por condomínio
                    dado_list = []
                    print("Condomínio:", d2)
                    for d1 in data:
                        # Alterando o formato da data por questão da API.
                        data_periodo = d1.strftime("%m/%d/%Y")
                        # Criando a URL para buscar na API por condomínio e por dia.
                        url_completa = (
                            self.url_job
                            + f"idCondominio={d2}&dtInicio={data_periodo}&dtFim={data_periodo}&agrupadoPorMes=0"
                        )
                        response = ut.api(
                            method="GET", url=url_completa, headers=self.header_job
                        )
                        if response.status_code == 200:
                            response_json = response.json()
                            if response_json:
                                for item in response_json[0]["itens"]:
                                    # Inserindo a data nos dados
                                    item[0]["data"] = d1.strftime("%Y-%m-%d")
                                    # Inserindo o numero do condomínio
                                    item[0]["id_condominio"] = d2
                                    # Adicionado o dado na lista.
                                    dado_list.extend(item)
                    print(f"Obteve {len(dado_list)} dados do condomínio {d2}")
                    if dado_list:
                        # Transformado a lista em Dataframe Pandas.
                        df_relatorio_receita_despesa = pd.DataFrame(dado_list)
                        # Inserindo na tabela staging
                        df_relatorio_receita_despesa.to_sql(
                            table,
                            engine,
                            schema=schema,
                            if_exists="append",
                            index=False,
                        )
            except Exception as ex:
                # Condição que atualizará o último condomínio na variável do Airflow caso dê falha no Job.
                update_variable = {
                    "schedule_dag": dt_execucao.strftime("%Y-%m-%d"),
                    "id_condominio": d2,
                }
                Variable.update(
                    "condominios_atualizacao", update_variable, serialize_json=True
                )
                raise print(f"ERROR! Causa: {ex}")

        # Analise para verificar se já teve tentativa na mesma data de execução.
        if data_ult_processamento == dt_execucao:
            # Delete dos dados possíveis processados do condomínio
            ut.delete_by_condition_pgsql(
                self.database_job,
                f"DELETE FROM {schema}.{table} WHERE id_condominio = {condominio_ult_processamento};",
            )

            print(
                f"Houve falha e continuará a partir do condomínio: {condominio_ult_processamento}"
            )
            dado_condominio_ajustado = dado_condominio[
                dado_condominio.index(condominio_ult_processamento) :
            ]
            _processamento_condominios(dado_condominio_ajustado)
        else:
            # Truncate na staging
            ut.truncate_pgsql(self.database_job, table=f"{schema}.{table}")

            print(f"Será processados {len(dado_condominio)} condomínios")
            _processamento_condominios(dado_condominio)


class Jobs_contabilidade:
    def __init__(self, datalake):
        self.datalake_conn = datalake

    def extract_data(self, conn_engine: str, conn_read: str, table: str, schema: str):
        # Conexão com o banco de dados de leitura
        connection = ut.obter_conn_uri(conn_read)
        engine = create_engine(
            f'{conn_engine}://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}'
        )
        # Conexão com o banco de dados de escrita
        connect_datalake = ut.obter_conn_uri(self.datalake_conn)
        engine_datalake = create_engine(
            f'postgresql://{connect_datalake["user"]}:{connect_datalake["password"]}@{connect_datalake["host"]}:{connect_datalake["port"]}/{connect_datalake["schema"]}'
        )
        # Registros na tabela
        df_count = pd.read_sql_query(
            f"SELECT COUNT(1) as count FROM {table}", con=engine
        )
        # Query referente ao count
        query = f"SELECT * FROM {table}"
        # Numeros registros retornados
        num_linhas = df_count["count"].item()
        # Condição para quando houver muitos registros
        if num_linhas >= 300000:
            total_por_pagina = 50000
            total_paginas = math.ceil(num_linhas / total_por_pagina)
            for pagina in range(1, total_paginas + 1):
                print(f"pagina {pagina} de {total_paginas}")
                limite_pagina = (pagina - 1) * total_por_pagina
                query_exec_pagina = (
                    query
                    + f" OFFSET {limite_pagina} ROWS FETCH NEXT {total_por_pagina} ROWS ONLY"
                )
                df = pd.read_sql_query(sql=query_exec_pagina, con=engine)
                # Condição para o primeiro registro recriar a estrutura da tabela novamente
                if pagina == 1:
                    df.to_sql(
                        table,
                        engine_datalake,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                    )
                else:
                    df.to_sql(
                        table,
                        engine_datalake,
                        schema=schema,
                        if_exists="append",
                        index=False,
                    )
        else:
            df = pd.read_sql_query(query, con=engine)
            df.to_sql(
                table, engine_datalake, schema=schema, if_exists="replace", index=False
            )


class Questor_OMIE:
    def __init__(
        self,
        schema: str = None,
        conn_questor: str = None,
        conn_datalake: str = None,
        table: str = None,
    ):
        self.schema = schema
        self.table = table
        self.conn_questor = conn_questor
        self.conn_datalake = conn_datalake

    def questor(self) -> list:
        query = f"SELECT t.*, current_date as data_execucao FROM {self.table} t;"
        return ut.read_firebird(database_id=self.conn_questor, query=query)

    def datalake(self):
        connection = ut.obter_conn_uri(self.conn_datalake)
        engine = create_engine(
            f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}'
        )

        try:
            df = pd.DataFrame(self.questor())
            df.to_sql(
                self.table, engine, schema=self.schema, if_exists="replace", index=False
            )
        except pd.errors.EmptyDataError as ex:
            print(f"Os dados estão vazios: {ex}")

    def omie(
        self,
        data_competencia: str,
        url_contrato: str,
        url_servico: str,
        app_key: str,
        app_secret: str,
        codigo_servico: str,
    ):
        query_folha = f"SELECT * FROM CONJEL.FATO_CALCULO_FOLHA where data_inicial_folha = date_trunc('Month', cast('{data_competencia}' as date)) - interval '1 Month'"
        print(
            "Executando a query que retornará a informação que será atualizada na API"
        )
        consulta_folha = ut.read_pgsql(
            database_id=self.conn_datalake, query=query_folha
        )
        print(f"Consulta obteve {len(consulta_folha)} registros!")

        def omie_api(url: str, data_call: str, parametros: list):
            headers = {"Content-Type": "application/json"}
            data_json = {
                "call": data_call,
                "app_key": app_key,
                "app_secret": app_secret,
                "param": parametros,
            }
            url_api = ut.api(method="POST", url=url, headers=headers, json=data_json)
            return url_api

        def processamento_api():
            print("Iniciando processamento da API!")
            atualizado = []
            falha = []

            if consulta_folha:
                for i in consulta_folha:
                    api_post_contrato = omie_api(
                        url_contrato,
                        data_call="ListarContratos",
                        parametros=[{"filtrar_cnpj_cpf": i[4]}],
                    )
                    api_post_servico = omie_api(
                        url_servico,
                        data_call="ListarCadastroServico",
                        parametros=[{"cCodigo": codigo_servico}],
                    )
                    api_post_servico_json = api_post_servico.json()["cadastros"][0][
                        "intListar"
                    ]["nCodServ"]
                    try:
                        if api_post_contrato.status_code == 200:
                            api_post_json = api_post_contrato.json()["contratoCadastro"]
                            contrato_filtrado = []
                            for cabecalho in api_post_json:
                                api_post_numero_contrato = omie_api(
                                    url_contrato,
                                    data_call="ConsultarContrato",
                                    parametros=[
                                        {
                                            "contratoChave": {
                                                "nCodCtr": cabecalho["cabecalho"][
                                                    "nCodCtr"
                                                ]
                                            }
                                        }
                                    ],
                                )
                                if api_post_numero_contrato.status_code == 200:
                                    if (
                                        api_post_numero_contrato.json()[
                                            "contratoCadastro"
                                        ]["cabecalho"]["cNumCtr"]
                                        == i[3]
                                    ):
                                        contrato_filtrado.append(
                                            {
                                                "cabecalho": api_post_numero_contrato.json()[
                                                    "contratoCadastro"
                                                ][
                                                    "cabecalho"
                                                ],
                                                "itensContrato": api_post_numero_contrato.json()[
                                                    "contratoCadastro"
                                                ][
                                                    "itensContrato"
                                                ],
                                                "infAdic": api_post_numero_contrato.json()[
                                                    "contratoCadastro"
                                                ][
                                                    "infAdic"
                                                ],
                                            }
                                        )
                                else:
                                    falha.append(
                                        {
                                            "cnpj_cpf": i[4],
                                            "contrato": cabecalho["cabecalho"][
                                                "cNumCtr"
                                            ],
                                            "detalhe": f"Não encontrado o contrato! \n {api_post_numero_contrato.text}",
                                            "etapa": "Buscar o contrato",
                                        }
                                    )
                            if contrato_filtrado:
                                api_post_numero_contrato_json = contrato_filtrado[0]
                                for item in api_post_numero_contrato_json[
                                    "itensContrato"
                                ]:
                                    if "itemOutrasInf" in item:
                                        del item["itemOutrasInf"]
                                    if (
                                        item["itemCabecalho"]["codServico"]
                                        == api_post_servico_json
                                    ):
                                        item["itemCabecalho"]["quant"] = i[5]
                                        if "itemOutrasInf" in item:
                                            del item["itemOutrasInf"]
                                omie_api(
                                    url=url_contrato,
                                    data_call="UpsertContrato",
                                    parametros=[api_post_numero_contrato_json],
                                )
                                atualizado.append(
                                    {"cnpj_cpf": i[4], "contrato": i[3], "folhas": i[5]}
                                )
                                if (
                                    next(
                                        (
                                            item
                                            for item in atualizado
                                            if item["cnpj_cpf"] == i[4]
                                        ),
                                        None,
                                    )
                                    is None
                                ):
                                    falha.append(
                                        {
                                            "cnpj_cpf": i[4],
                                            "contrato": i[3],
                                            "detalhe": "Não encontrado o item no contrato!",
                                            "etapa": "Buscar o item no contrato",
                                        }
                                    )
                            if (
                                next(
                                    (
                                        item
                                        for item in atualizado
                                        if item["cnpj_cpf"] == i[4]
                                    ),
                                    None,
                                )
                                is None
                            ):
                                falha.append(
                                    {
                                        "cnpj_cpf": i[4],
                                        "contrato": i[3],
                                        "detalhe": "Não encontrado o contrato!",
                                        "etapa": "Buscar o contrato",
                                    }
                                )
                        else:
                            falha.append(
                                {
                                    "cnpj_cpj": i[4],
                                    "contrato": i[3],
                                    "detalhe": api_post_contrato.text,
                                    "etapa": "Buscar o contrato do cliente",
                                }
                            )
                    except Exception as ex:
                        print(ex, i[0])
                return {"atualizado": atualizado, "falha": falha}
            else:
                raise print("Não retornou dados da consulta SQL")

        return processamento_api()
