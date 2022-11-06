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


class Jobs_c8sgestao:

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
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
        df_condominio.to_sql(table, engine, schema=schema, if_exists="replace", index=False)

    def st_relatorio_receita_despesa(self, table: str, data_execucao: str, intervalo_execucao: int, schema: str):
        # Obtendo a data de execução do Scheduler e diminuindo pelos numeros de meses parametrizados no Airflow.

        # Transformando a string em data
        dt_execucao = datetime.strptime(data_execucao, "%Y-%m-%d")
        # Variável que armazena a última data de processamento e o último condomínio processado
        variable_processamento = Variable.get("condominios_atualizacao", deserialize_json=True)
        data_ult_processamento = datetime.strptime(variable_processamento["schedule_dag"], "%Y-%m-%d")
        condominio_ult_processamento = variable_processamento["id_condominio"]
        # Dimunuindo os números de meses para reprocessamento.
        data_inicio = dt_execucao.replace(day=1) - relativedelta(months=intervalo_execucao)
        data_fim = dt_execucao
        # Criando range de datas para o laço de repetição.
        data = pd.date_range(data_inicio, data_fim, freq="D")
        print(f"Reprocessando entre os dias {data_inicio.strftime('%Y-%m-%d')} a {data_fim.strftime('%Y-%m-%d')}")

        # Query que busca no banco de dados os condomínios cadastrados para buscar na API.
        dado_condominio = ut.read_pgsql(self.database_job, "select array_agg(distinct id_condominio) from dim_condominio;")[0][0]

        # Obtendo a conexão cadastrada do PostgreSQL (Datalake) no Airflow.
        connection = ut.obter_conn_uri(self.database_job)
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')

        def _processamento_condominios(condominios):
            try:
                for d2 in condominios:
                    # Criado lista que será preenchida com os dados da API por condomínio
                    dado_list = list()
                    print("Condomínio:", d2)
                    for d1 in data:
                        # Alterando o formato da data por questão da API.
                        data_periodo = d1.strftime("%m/%d/%Y")
                        # Criando a URL para buscar na API por condomínio e por dia.
                        url_completa = self.url_job + f"idCondominio={d2}&dtInicio={data_periodo}&dtFim={data_periodo}&agrupadoPorMes=0"
                        response = ut.api(method="GET", url=url_completa, headers=self.header_job)
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
                    if len(dado_list) > 0:
                        # Transformado a lista em Dataframe Pandas.
                        df_relatorio_receita_despesa = pd.DataFrame(dado_list)
                        # Inserindo na tabela staging
                        df_relatorio_receita_despesa.to_sql(table, engine, schema=schema, if_exists='append', index=False)
            except Exception as ex:
                # Condição que atualizará o último condomínio na variável do Airflow caso dê falha no Job.
                update_variable = {"schedule_dag": dt_execucao.strftime("%Y-%m-%d"), "id_condominio": d2}
                Variable.update("condominios_atualizacao", update_variable, serialize_json=True)
                raise print(f"ERROR! Causa: {ex}")

    
        # Analise para verificar se já teve tentativa na mesma data de execução.
        if data_ult_processamento == dt_execucao:
            # Delete dos dados possíveis processados do condomínio
            ut.delete_by_condition_pgsql(self.database_job, f"DELETE FROM {schema + '.' + table} WHERE id_condominio = {condominio_ult_processamento};")

            print(f"Houve falha e continuará a partir do condomínio: {condominio_ult_processamento}")
            dado_condominio_ajustado = dado_condominio[dado_condominio.index(condominio_ult_processamento):]
            _processamento_condominios(dado_condominio_ajustado)
        else:
            # Truncate na staging
            ut.truncate_pgsql(self.database_job, table=schema + '.' + table)
            
            print(f"Será processados {len(dado_condominio)} condomínios")
            _processamento_condominios(dado_condominio)

class Jobs_conjel:

    def __init__(self, datalake):
        self.datalake_conn = datalake

    def read_pd_sql(self, type, conn, query: str) -> pd:
        try:
            if type == "postgres":
                connection = ut.obter_conn_uri(conn)
                engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
            if type == "mysql":
                connection = ut.obter_conn_uri(conn)
                engine = create_engine(f'mysql+mysqldb://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
            else:
                raise print("Tipo inválido!")
            df = pd.read_sql_query(query, con=engine)
        except pd.errors.EmptyDataError as ex:
            print(f"Os dados estão vazios: {ex}")
        except Exception as ex:
            print(f"Falha! Motivo: {ex}")
        return df

    def extract(self, conn_type, conn_read, query: str, table: str, schema: str):
        connection = ut.obter_conn_uri(self.datalake_conn)
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
        self.read_pd_sql(type=conn_type, conn=conn_read, query=query).to_sql(table, engine, schema=schema, if_exists="replace", index=False)

class Questor_OMIE:

    def __init__(self, schema: str = None, conn_questor: str = None, conn_datalake: str = None, table: str = None):
        self.schema = schema
        self.table = table
        self.conn_questor = conn_questor
        self.conn_datalake = conn_datalake

    def questor(self) -> list:
        query = f"SELECT t.*, current_date as data_execucao FROM {self.table} t;"
        consulta = ut.read_firebird(database_id=self.conn_questor, query=query)
        return consulta

    def datalake(self):
        try: 
            df = pd.DataFrame(self.questor())
        except pd.errors.EmptyDataError as ex:
            print(f"Os dados estão vazios: {ex}")
        
        connection = ut.obter_conn_uri(self.conn_datalake)
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
        df.to_sql(self.table, engine, schema=self.schema, if_exists="replace", index=False)

    def omie(self, data_competencia: str, url_contrato: str, url_cliente: str, headers: dict, app_key: str, app_secret: str, codigo_servio: str):
        print("Executando a query que retornará a informação que será atualizada na API.")
        query_folha = f"""
                        select
                            CASE WHEN f.inscrfederal = e.inscrfederal THEN f.inscrfederal
                                ELSE f.inscrfederal END CNPJ,
                            CASE WHEN count (distinct (c.codigofunccontr)) = 1 THEN 2
                                 ELSE count (distinct (c.codigofunccontr)) END as "folhas"
                        from CONJEL.QUESTOR_DIM_funcpercalculo c
                            inner join CONJEL.QUESTOR_DIM_periodocalculo p on p.codigoempresa = c.codigoempresa
                                                                          and p.codigopercalculo = c.codigopercalculo
                            inner join (select codigoempresa as codemp,
                                            codigofunccontr as codfunc,
                                            max (datatransf) as datafunc
                                        from CONJEL.QUESTOR_DIM_funclocal
                                        group by 1,2) h on h.codemp = c.codigoempresa
                                                       and h.codfunc = c.codigofunccontr
                            inner join CONJEL.QUESTOR_DIM_funclocal l on l.codigoempresa = c.codigoempresa
                                                                     and l.codigofunccontr = c.codigofunccontr
                                                                     and l.datatransf = h.datafunc
                            inner join CONJEL.QUESTOR_DIM_estab e on e.codigoempresa = l.codigoempresa
                                                                 and e.codigoestab = l.codigoestab
                            inner join CONJEL.QUESTOR_DIM_estab f on f.codigoempresa = c.codigoempresa
                                                                 and f.codigoestab = 1
                            inner join CONJEL.QUESTOR_DIM_usuario u on u.codigousuario = c.codigousuario
                        where p.datainicialfolha = cast('{data_competencia}' as date) - interval '1 Month'
                        group by 1"""
        consulta_folha = ut.read_pgsql(database_id=self.conn_datalake, query=query_folha)
        
        
        def omie_api(url: str, data_call: str, parametros: list):
            data_json = {
                "call": data_call,
                "app_key": app_key,
                "app_secret": app_secret,
                "param": parametros
            }
            url_api = ut.api(method="POST", url=url, headers=headers, json=data_json)
            return url_api
        
        
        def processamento_api(url_contrato_api: str, url_cliente_api: str, codigo_servico_api: str):
            print("Iniciando processamento da API!")
            contrato_cadastro = []
            falha = []

            try:
                for i in consulta_folha:
                    print("Buscando dados dos clientes!")
                    api_post_cliente = omie_api(url_cliente_api, data_call="ListarClientesResumido", parametros=[{"clientesFiltro": {"cnpj_cpf": i[0]}}])
                    if api_post_cliente.status_code == 200:
                        print("Buscando dados dos contratos do cliente!")
                        info_cliente = {"cnpj_cpf": i[0], "codigo_cliente": api_post_cliente.json()["clientes_cadastro_resumido"][0]["codigo_cliente"]}
                        api_post_contrato = omie_api(url_contrato_api, data_call="ListarContratos", parametros=[{"filtrar_cliente": info_cliente["codigo_cliente"]}])
                        try:
                            if api_post_contrato.status_code == 200:
                                api_post_json = api_post_contrato.json()["contratoCadastro"]
                                for item in api_post_json[0]["itensContrato"]:
                                    if item["itemCabecalho"]["codServMunic"] == codigo_servico_api:
                                        item["itemCabecalho"]["quant"] = i[1]
                                    else:
                                        falha.extend({"cnpj_cpf": i[0], "detalhe": "Não encontrado o item no contrato!", "etapa": "Busca o item no contrato"})
                                contrato_cadastro.extend(api_post_json)
                            else:
                                falha.extend({"cnpj_cpj": i[0], "detalhe": api_post_contrato.text, "etapa": "Busca o contrato do cliente"})
                            omie_api(url=url_contrato_api, data_call="AlterarContrato", parametros=contrato_cadastro)
                        except Exception as ex:
                            print(ex, i[0])
                    else:
                        falha.extend({"cnpj_cnpj": i[0], "detalhe": api_post_cliente.text, "etapa": "Busca cadastro do cliente"})
                return falha
            except Exception as ex:
                print(f"Falha! Motivo: {ex}")
        
        return processamento_api(url_cliente_api=url_cliente, url_contrato_api=url_contrato, codigo_servico_api=codigo_servio)
