from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator


def dimensoes_tareffa():

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    task_dimensao_empresas = PostgresOperator(
            task_id="dimensao_empresas",
            postgres_conn_id="postgres-datalake",
            sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas;",
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
                        """],
            autocommit=True
        )

    task_dimensao_regime = PostgresOperator(
        task_id="dimensao_regime",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_regime;",
            """INSERT INTO conjel.tareffa_dim_regime
                SELECT distinct idregime as id, regimedescricao as descricao 
                FROM staging.view_empresas ve;
            """],
        autocommit=True
    )

    task_dimensao_empresas_caracteristicas = PostgresOperator(
        task_id="dimensao_empresas_caracteristicas",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_empresas_caracteristicas;",
            """INSERT INTO conjel.tareffa_dim_empresas_caracteristicas
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
            """INSERT INTO conjel.tareffa_dim_grupo_caracteristica
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
            """INSERT INTO conjel.tareffa_dim_caracteristica
                SELECT distinct idcaracteristica as id, caracteristica as descricao
                FROM staging.view_empresas_caracteristicas vec;
            """],
        autocommit=True
    )
    
    task_dimensao_departamento = PostgresOperator(
        task_id="dimensao_departamento",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE conjel.tareffa_dim_departamento;",
            """INSERT INTO conjel.tareffa_dim_departamento
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
            """INSERT INTO conjel.tareffa_dim_empresas_servicos
                SELECT
                    idempresa as id_empresa, idservico as id_servico,
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
            """INSERT INTO conjel.tareffa_dim_servicos
                SELECT distinct idservico as id, servico as descricao
                FROM staging.view_empresas_servicos ves;
            """],
        autocommit=True
    )

    inicio >> [task_dimensao_empresas, task_dimensao_regime, task_dimensao_empresas_caracteristicas, 
               task_dimensao_grupo_caracteristica, task_dimensao_caracteristica, task_dimensao_departamento,
               task_dimensao_empresas_servicos, task_dimensao_servicos] >> fim

def dimensoes_questor():
    inicio = DummyOperator(task_id="inicio")

    task_periodocalculo = PostgresOperator(
        task_id="periodocalculo",
        postgres_conn_id="postgres-datalake",
        sql=["""
            INSERT INTO CONJEL.QUESTOR_DIM_periodocalculo
            select
                "0" as CODIGOEMPRESA, "1" as CODIGOPERCALCULO, "2" as CODIGOTIPOCALC, "3" as COMPET, "4" as DATAINICIALFOLHA,
                "5" as DATAFINALFOLHA, "6" as DATAPGTO, "7" as TIPOREGRATRIBCOMPL, "8" as SEQ, "9" as DATAPGTODIRET,
                "10" as CODIGOEMPRESACOMPL, "11" as CODIGOPERCALCCOMPL, "12" as CODIGOEMPRESAAGRUPADOR, "13" as CODIGOPERCALCULOAGRUPADOR,
                "14" as TIPOINTEGRACAO, "15" as TIPODCTOCOMPL, "16" as TIPOAPURACAOANTERIOR, "17" as POSSUIPARCELAMENTO,
                cast("18" as integer) as CODIGOSINDDISSIDIOEMP, "19" as FECHADO, "20" as DATA_EXECUCAO
            from staging.periodocalculo p
            /* Esse filtro buscará somente o último cálculo da folha */
            where "1" = (select max("1") from staging.periodocalculo);
            """],
        autocommit=True
    )

    task_funcpercalculo = PostgresOperator(
        task_id="funcpercalculo",
        postgres_conn_id="postgres-datalake",
        sql=["""
            INSERT INTO CONJEL.QUESTOR_DIM_FUNCPERCALCULO
            select 
                "0" as CODIGOEMPRESA, "1" as CODIGOPERCALCULO, "2" as CODIGOFUNCCONTR, "3" as DATACARGO,
                "4" as DATAESCALA, "5" as DATALOCAL, "6" as CODIGOEMPRESAIMP, "7" as DATASALARIO,
                "8" as CODIGOSIND, "9" as DATAPGTOFOLHA, "10" as SEQCALCULO, "11" as CODIGOPERCALCIMP,
                "12" as EMITEENVEL, "13" as CODIGOUSUARIO, "14" as DATAHORALCTO, "15" as DATASINDCONVENCAO,
                "16" as DATAFUNCSINDICATO, "17" as ORIGEMDADO, "18" as DATASINDCONTRIB1, "19" as DATASINDCONTRIB2,
                "20" as DATASINDCONTRIB3, "21" as DATASINDCONTRIB4, "22" as DATASINDCONTRIB5, "23" as DATASINDCONTRIB6,
                "24" as IDFPC, "25" as INDMV, "26" as DATAESCALAVALETRANSP, "27" as GEROUTRANSACAOESOCIAL1200,
                "28" as GERARESOCIAL, "29" as GEROUTRANSACAOESOCIAL1210, "30" as SEQFUNCSALARIO, "31" as DATA_EXECUCAO
            from staging.funcpercalculo
            /* Esse filtro buscará somente o último cálculo da folha */
            where "1" = (select max("1") from staging.funcpercalculo);
            """],
        autocommit=True
    )

    task_estab = PostgresOperator(
        task_id="estab",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE CONJEL.QUESTOR_DIM_ESTAB",
            """
            INSERT INTO CONJEL.QUESTOR_DIM_ESTAB
            select
                "0" as CODIGOEMPRESA, "1" as CODIGOESTAB, "2" as DATAINICIAL, "3" as NOMEESTA, "4" as NOMEESTABCOMPLETO,
                "5" as NOMEFANTASIA, "6" as APELIDOESTAB, "7" as CODIGOTIPOLOGRAD, "8" as ENDERECOESTA, "9" as NUMENDERESTA,
                "10" as COMPLENDERESTAB, "11" as BAIRROENDERESTAB, "12" as DATAALTERENDER, "13" as SIGLAESTADO, "14" as CODIGOMUNIC,
                "15" as CEPENDERESTAB, "16" as DDDFONE, "17" as NUMEROFONE, "18" as DDDFAX, "19" as NUMEROFAX, "20" as CAIXAPOSTAL,
                "21" as SIGLAESTADOCXP, "22" as CEPCAIXAPOSTAL, "23" as EMAILDPO, "24" as EMAIL, "25" as PAGINAINTERNET,
                "26" as DATAINICIOATIV, "27" as DATAENCERATIV, "28" as SOCIEDADECONTAPARTICIPACAO, "29" as INSCRICAOSCP,
                "30" as TIPOINSCR, "31" as INSCRFEDERAL, "32" as CPFRESPCNO, "33" as SUFRAMA, "34" as CODIGONATURJURID,
                "35" as CODIGOATIVFEDERAL, "36" as DESCRATIVFEDESTAB, "37" as DATAALTERATIVFED, "38" as TIPOREGIST,
                "39" as NUMEROREGIST, "40" as DATAREGIST, "41" as OBSERVREGIST, "42" as INSCRESTAD, "43" as CODIGOATIVESTAD,
                "44" as DESCRATIVESTESTAB, "45" as INSCRMUNIC, "46" as CODIGOATIVMUNIC, "47" as INSCRIMOBILIARIA,
                "48" as ESPECIEESTAB, "49" as INSCRBANCOCENTRAL, "50" as INSCRSUSEP, "51" as DESCRATIVMUNESTAB,
                "52" as PORTEEMPRESA, "53" as INSCRCVM, "54" as CODIGOTABFERIADO, cast("55" as numeric(14,2)) as VALORNOMINALCOTAS,
                "56" as CAEPF, "57" as NOMEAUDITOR, "58" as CVMAUDITOR , "59" as INSCRFEDERALAUDITOR,
                cast("60" as numeric(14,2)) as CAPITALSOCIAL, "61" as INSCRCAEPF, "62" as INSCRFEDERALPRODRURAL, cast("63" as integer) as CODIGOCATEGEMPRESACLIENTE,
                "64" as CERTIFICADO, "65" as STATUSSINCZEN, "66" as TIPOALTERACAOESOCIAL
            from staging.estab;
            """
        ],
    autocommit=True
    )

    task_usuario = PostgresOperator(
        task_id="usuario",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE CONJEL.QUESTOR_DIM_USUARIO",
            """
            INSERT INTO CONJEL.QUESTOR_DIM_USUARIO
            select 
                "0" as CODIGOUSUARIO, "1" as NOMEUSUARIO, "2" as NOMEUSUARIOCOMPL, "3" as SENHAUSUARIO,
                "4" as TEMPOTOLERANCIA, "5" as HORADIARIA, cast("6" as numeric(14,2)) as CUSTOHORA, "7" as NIVELUSUARIO,
                "8" as FORMAAVISOAGENDA, "9" as DATABAIXAUSUARIO, "10" as EMAILUSUARIO, "11" as FILTROEMPRESA,
                "12" as PERFILTAREFFA, "13" as EDOCCONSULTARDOCSRECEBIDOS, "14" as EDOCINTERVCONSULTAS,
                "15" as EDOCAUTH, "16" as EDOCDATAHORAULTIMACONSULTA, "17" as DATAALTERACAO, "18" as HABILITARCHATONLINE,
                cast("19" as bytea) as FOTOUSUARIO, "20" as STATUSSINCZEN, "21" as ACEITETERMO
            from staging.usuario;
            """
        ],
        autocommit=True
    )

    task_funclocal = PostgresOperator(
        task_id="funclocal",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE CONJEL.QUESTOR_DIM_FUNCLOCAL",
            """
            INSERT INTO CONJEL.QUESTOR_DIM_FUNCLOCAL
            select 
                "0" as CODIGOEMPRESA, "1" as CODIGOFUNCCONTR, "2" as DATATRANSF, "3" as CODIGOESTAB, "4" as CLASSIFORGAN,
                "5" as LIVROREG, "6" as FOLHALIVROREG, "7" as FICHAREG, "8" as DATAHORACADAST, "9" as CODIGOMOTIVO,
                "10" as TIPOADMRAIS, "11" as TIPOADMCAGED, "12" as TIPOAFASTSEFIP, "13" as TIPOAFASTRAIS, "14" as TIPOAFASTCAGED,
                "15" as TIPOADM, "16" as TIPOTRANSF, "17" as INDICATADM, "18" as NUMEROPROCESSO, "19" as TIPOALTERACAOESOCIAL
            from staging.funclocal;
            """
        ],
        autocommit=True
    )

    task_empresasegmento = PostgresOperator(
        task_id="empresasegmento",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE CONJEL.QUESTOR_DIM_EMPRESASEGMENTO",
            """
            INSERT INTO CONJEL.QUESTOR_DIM_EMPRESASEGMENTO
            select
                "0" as codigoempresa, "1" as seq, cast("2" as date) as DATAINICIOSEGMENTO, "3" as CODIGOESTAB, "4" as CODIGOSEGMENTO,
                cast("5" as date) as DATAFIMSEGMENTO, "6" as OBSERVACAOSEGMENTO, "7" as CODIGOUSUARIO, "8" as DATAHORAALTERACAO
            from staging.empresasegmento;
            """
        ],
        autocommit=True
    )

    task_empresa = PostgresOperator(
        task_id="empresa",
        postgres_conn_id="postgres-datalake",
        sql=["TRUNCATE TABLE CONJEL.QUESTOR_DIM_EMPRESA",
            """
            INSERT INTO CONJEL.QUESTOR_DIM_EMPRESA
            select 
                "0" as codigoempresa, cast("1" as BYTEA) as MARCADAGUAEMPRESA, "2" as NOMEEMPRESA, cast("3" as BYTEA) as LOGOTIPOEMPRESA
            from staging.empresa;
            """
        ],
        autocommit=True
    )

    fim = DummyOperator(task_id="fim")

    inicio >> [task_periodocalculo, task_estab, task_funcpercalculo, task_usuario, task_funclocal, task_empresasegmento, task_empresa] >> fim


def fato_calculo_folha(data_competencia):

    
    task_fato_calculo_folha = PostgresOperator(
        task_id="fato_calculo_folha",
        postgres_conn_id="postgres-datalake",
        sql=["DELETE FROM CONJEL.FATO_CALCULO_FOLHA WHERE DATA_PROCESSAMENTO = CURRENT_DATE",
            f"""
            INSERT INTO CONJEL.FATO_CALCULO_FOLHA
            SELECT
                current_date as data_processamento,
                datainicialfolha,
                e.nomeempresa as empresa,
                substring(observacaosegmento from position('Contrato:' in observacaosegmento) +9 for 10) as contrato,
                substring(observacaosegmento from position('CNPJ:' in observacaosegmento) +5) as cnpj,
                count(distinct (c.codigofunccontr)) as folhas_apuradas
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
            inner join CONJEL.QUESTOR_DIM_empresa e on e.codigoempresa = c.codigoempresa
            inner join CONJEL.QUESTOR_DIM_usuario u on u.codigousuario = c.codigousuario
            inner join CONJEL.QUESTOR_DIM_empresasegmento es on es.codigoempresa = c.codigoempresa 
                                                            and es.CODIGOSEGMENTO in (19)
                                                            and es.datafimsegmento is null
            where p.datainicialfolha = date_trunc('Month', cast('{data_competencia}' as date)) - interval '1 Month'
            group by 1,2,3,4,5
        """]
    )

    task_fato_calculo_folha
