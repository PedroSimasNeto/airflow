TRUNCATE TABLE CONJEL.QUESTOR_DIM_ESTAB;
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