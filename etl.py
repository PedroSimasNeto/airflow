import requests
import pandas as pd
from sqlalchemy import create_engine
import utils as ut


class jobs:

    def __init__(self, url, header, database):
        self.url_job = url
        self.header_job = header
        self.database_job = database

    def importar_condominios(self, table: str):
        response = requests.request("GET", self.url_job, headers=self.header_job)
        df_condominio = pd.DataFrame(response.json())

        connection = ut.obter_conn_uri(self.database_job)
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
        ut.truncate_pgsql(self.database_job, table)
        df_condominio.to_sql(table, engine, if_exists="append", index=False)

    def relatorio_receita_despesa(self, table: str, range_date: dict):
        data = pd.date_range(range_date["inicio"], range_date["fim"], freq="D")
        dado_list = list()

        dado = ut.read_pgsql(self.database_job, "select array_agg(distinct id_condominio_cond) from condominio;")[0][0]

        try:
            for d2 in dado:
                print("Condomínio:", d2)
                for d1 in data:
                    data_periodo = d1.strftime("%d/%m/%Y")
                    url_completa = self.url_job + f"idCondominio={d2}&dtInicio={data_periodo}&dtFim={data_periodo}&agrupadoPorMes=0"
                    response = requests.request("GET", url_completa, headers=self.header_job)
                    if response.status_code and response.status_code == 200:
                        response_json = response.json()
                        if response_json:
                            for item in response_json[0]["itens"]:
                                item[0]["data"] = d1.strftime("%Y-%m-%d")
                                dado_list.extend(item)
        except Exception as ex:
            raise print(f"ERRO! Motivo: {ex}")

        df_relatorio_receita_despesa = pd.DataFrame(dado_list)
        connection = ut.obter_conn_uri(self.database_job)
        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["host"]}:{connection["port"]}/{connection["schema"]}')
        df_relatorio_receita_despesa.to_sql(table, engine, if_exists="append", index=False)
