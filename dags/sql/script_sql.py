from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator


def dimensoes_tareffa():

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    task_dimensao_empresas = PostgresOperator(
        task_id="dimensao_empresas",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_empresas;",
            """INSERT INTO conjel.tareffa_dim_empresas
                    SELECT
                        idempresa as id_empresa, codigoquestor as codigo_questor, razaosocial as razao_social,
                        cnpj, idcnae as id_cnae, cast(inicioprestacaoservicos as date) as inicio_prestacao_servicos,
                        case when ematriz is true then 1
                            else 0 end as matriz,
                        inscricaoestadual as inscricao_estadual,
                        case when statusativa is true then 1
                            else 0 end status_ativa,
                        idregime as id_regime
                    FROM staging.view_empresas ve;
                        """,
        ],
        autocommit=True,
    )

    task_dimensao_regime = PostgresOperator(
        task_id="dimensao_regime",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_regime;",
            """INSERT INTO conjel.tareffa_dim_regime
                SELECT distinct idregime as id, regimedescricao as descricao 
                FROM staging.view_empresas ve;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_empresas_caracteristicas = PostgresOperator(
        task_id="dimensao_empresas_caracteristicas",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_empresas_caracteristicas;",
            """INSERT INTO conjel.tareffa_dim_empresas_caracteristicas
                SELECT
                    idempresa as id_empresa,
                    idcaracteristica as id_caracteristica,
                    cast(split_part(grupocaracteristica, '.', 1) as smallint) as id_grupo_caracteristica
                FROM staging.view_empresas_caracteristicas vec;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_grupo_caracteristica = PostgresOperator(
        task_id="dimensao_grupo_caracteristica",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_grupo_caracteristica;",
            """INSERT INTO conjel.tareffa_dim_grupo_caracteristica
                SELECT distinct
                    cast(split_part(grupocaracteristica, '.', 1) as smallint) as id,
                    idempresa as id_empresa, 
                    trim(substring(grupocaracteristica FROM position('.' in grupocaracteristica) + 1 for length(grupocaracteristica))) as descricao
                FROM staging.view_empresas_caracteristicas vec;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_caracteristica = PostgresOperator(
        task_id="dimensao_caracteristica",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_caracteristica;",
            """INSERT INTO conjel.tareffa_dim_caracteristica
                SELECT distinct idcaracteristica as id, caracteristica as descricao
                FROM staging.view_empresas_caracteristicas vec;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_departamento = PostgresOperator(
        task_id="dimensao_departamento",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_departamento;",
            """INSERT INTO conjel.tareffa_dim_departamento
                with depart as (
                    SELECT distinct departamento as descricao
                    FROM staging.view_empresas_responsaveis ver)
                SELECT row_number() over (order by descricao) as id, descricao FROM depart;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_empresas_servicos = PostgresOperator(
        task_id="dimensao_empresas_servicos",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_empresas_servicos;",
            """INSERT INTO conjel.tareffa_dim_empresas_servicos
                SELECT
                    idempresa as id_empresa, idservico as id_servico,
                    case when ativo is true then 1
                         else 0 end as ativo,
                    vencimento_alternativo
                FROM staging.view_empresas_servicos ves;
            """,
        ],
        autocommit=True,
    )

    task_dimensao_servicos = PostgresOperator(
        task_id="dimensao_servicos",
        postgres_conn_id="postgres-datalake",
        sql=[
            "TRUNCATE TABLE conjel.tareffa_dim_servicos;",
            """INSERT INTO conjel.tareffa_dim_servicos
                SELECT distinct idservico as id, servico as descricao
                FROM staging.view_empresas_servicos ves;
            """,
        ],
        autocommit=True,
    )

    (
        inicio
        >> [
            task_dimensao_empresas,
            task_dimensao_regime,
            task_dimensao_empresas_caracteristicas,
            task_dimensao_grupo_caracteristica,
            task_dimensao_caracteristica,
            task_dimensao_departamento,
            task_dimensao_empresas_servicos,
            task_dimensao_servicos,
        ]
        >> fim
    )


def dimensoes_questor():
    inicio = EmptyOperator(task_id="inicio")

    task_periodocalculo = PostgresOperator(
        task_id="periodocalculo",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/periodocalculo.sql",
        autocommit=True,
    )

    task_funcpercalculo = PostgresOperator(
        task_id="funcpercalculo",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/funcpercalculo.sql",
        autocommit=True,
    )

    task_estab = PostgresOperator(
        task_id="estab",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/estab.sql",
        autocommit=True,
    )

    task_usuario = PostgresOperator(
        task_id="usuario",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/usuario.sql",
        autocommit=True,
    )

    task_funclocal = PostgresOperator(
        task_id="funclocal",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/funclocal.sql",
        autocommit=True,
    )

    task_empresasegmento = PostgresOperator(
        task_id="empresasegmento",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/empresasegmento.sql",
        autocommit=True,
    )

    task_empresa = PostgresOperator(
        task_id="empresa",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/empresa.sql",
        autocommit=True,
    )

    task_terceiropgto = PostgresOperator(
        task_id="terceiropgto",
        postgres_conn_id="postgres-datalake",
        sql="sql/questor/terceiropgto.sql",
        autocommit=True,
    )

    fim = EmptyOperator(task_id="fim")

    (
        inicio
        >> [
            task_periodocalculo,
            task_estab,
            task_funcpercalculo,
            task_usuario,
            task_funclocal,
            task_empresasegmento,
            task_empresa,
            task_terceiropgto,
        ]
        >> fim
    )


def fato_omie():

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    data_competencia = "{{ next_ds }}"

    task_fato_calculo_folha_condominio = PostgresOperator(
        task_id="fato_calculo_folha_condominio",
        postgres_conn_id="postgres-datalake",
        sql="sql/fato_omie/fato_calculo_folha_condominio.sql",
        parameters={"data_competencia": data_competencia},
        autocommit=True,
    )

    task_fato_calculo_folha_conjel = PostgresOperator(
        task_id="fato_calculo_folha_conjel",
        postgres_conn_id="postgres-datalake",
        sql="sql/fato_omie/fato_calculo_folha_conjel.sql",
        parameters={"data_competencia": data_competencia},
        autocommit=True,
    )

    (
        inicio
        >> [task_fato_calculo_folha_condominio, task_fato_calculo_folha_conjel]
        >> fim
    )
