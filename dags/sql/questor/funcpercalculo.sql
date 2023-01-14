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
from staging.funcpercalculo p1
/* Esse filtro buscará somente o último cálculo da folha */
where not exists (select 1 from CONJEL.QUESTOR_DIM_FUNCPERCALCULO p4 where p4.codigoempresa = p1."0"
                                                                        and p4.codigopercalculo = p1."1"
                                                                        and p4.codigofunccontr = p1."2");