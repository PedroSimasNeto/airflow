INSERT INTO CONJEL.QUESTOR_DIM_periodocalculo
select
    "0" as CODIGOEMPRESA, "1" as CODIGOPERCALCULO, "2" as CODIGOTIPOCALC, "3" as COMPET, "4" as DATAINICIALFOLHA,
    "5" as DATAFINALFOLHA, "6" as DATAPGTO, "7" as TIPOREGRATRIBCOMPL, "8" as SEQ, "9" as DATAPGTODIRET,
    "10" as CODIGOEMPRESACOMPL, "11" as CODIGOPERCALCCOMPL, "12" as CODIGOEMPRESAAGRUPADOR, "13" as CODIGOPERCALCULOAGRUPADOR,
    "14" as TIPOINTEGRACAO, "15" as TIPODCTOCOMPL, "16" as TIPOAPURACAOANTERIOR, "17" as POSSUIPARCELAMENTO,
    cast("18" as integer) as CODIGOSINDDISSIDIOEMP, "19" as FECHADO, "20" as DATA_EXECUCAO
from staging.periodocalculo p1
/* Esse filtro buscará somente o último cálculo da folha */
where not exists (select 1 from CONJEL.QUESTOR_DIM_periodocalculo p3 where p3.codigoempresa = p1."0" 
                                                                        and p3.codigopercalculo = p1."1");