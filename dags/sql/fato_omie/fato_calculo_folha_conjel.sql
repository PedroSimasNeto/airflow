DELETE FROM CONJEL.FATO_CALCULO_FOLHA WHERE (DATA_PROCESSAMENTO = CURRENT_DATE OR data_inicial_folha = date_trunc('Month', cast({{ params.data_competencia }} as date)) - interval '1 Month');
INSERT INTO CONJEL.FATO_CALCULO_FOLHA
SELECT
    'conjel_bc' as empresa,
    f.data_processamento,
    f.data_inicial_folha,
    f.cliente,
    f.contrato,
    f.cnpj,
    sum(case 
        when f.segmento = 14 and f.qnt_folhas < 2 then 2
        when f.segmento = 17 and f.qnt_folhas = 0 then 1
        else f.qnt_folhas end) as folhas_apuradas
FROM (
    SELECT
        current_date as data_processamento,	
        coalesce(datainicialfolha, date_trunc('Month', cast({{ params.data_competencia }} as date)) - interval '1 Month') as data_inicial_folha,
        e.nomeempresa as cliente,
        substring(observacaosegmento from position('Contrato:' in observacaosegmento) +9 for position(',' in observacaosegmento) - 10) as contrato,
        substring(observacaosegmento from position('CNPJ:' in observacaosegmento) +5) as cnpj,
        codigosegmento as segmento,
        count(distinct (c.codigofunccontr)) as qnt_folhas
    FROM CONJEL.QUESTOR_DIM_EMPRESASEGMENTO es
        inner join CONJEL.QUESTOR_DIM_EMPRESA e on e.codigoempresa = es.codigoempresa
        left join CONJEL.QUESTOR_DIM_PERIODOCALCULO p on p.codigoempresa = e.codigoempresa
                                                        and p.datainicialfolha = date_trunc('Month', cast({{ params.data_competencia }} as date)) - interval '1 Month'
        left join CONJEL.QUESTOR_DIM_FUNCPERCALCULO c on c.codigoempresa = p.codigoempresa
                                                        and c.codigopercalculo = p.codigopercalculo
    where es.codigosegmento in (14, 15, 17, 18)
        and es.datafimsegmento is null
    group by 2, 3, 4, 5, 6
    
    UNION ALL
    
    SELECT
        current_date as data_processamento,
        coalesce(t.compet, date_trunc('Month', cast({{ params.data_competencia }} as date)) - interval '1 Month'),
        e.nomeempresa as cliente,
        substring(observacaosegmento from position('Contrato:' in observacaosegmento) +9 for position(',' in observacaosegmento) - 10) as contrato,
        substring(observacaosegmento from position('CNPJ:' in observacaosegmento) +5) as cnpj,
        codigosegmento as segmento,
        count(distinct (t.seq)) as qnt_folhas
    FROM CONJEL.QUESTOR_DIM_EMPRESASEGMENTO es
        inner join CONJEL.QUESTOR_DIM_EMPRESA e on e.codigoempresa = es.codigoempresa
        left join (SELECT
                        tp.codigoempresa as codigoempresa,
                        tp.compet as compet,
                        tp.codigoterc as codigoterc,
                        cast(tp.compet as varchar) ||tp.codigoterc || tp.seq as seq
                    FROM CONJEL.QUESTOR_DIM_TERCEIROPGTO tp) t on t.codigoempresa = e.codigoempresa
                                                                and t.compet = date_trunc('Month', cast({{ params.data_competencia }} as date)) - interval '1 Month'                                                    
    where es.codigosegmento in (14, 15, 17, 18)
      and es.datafimsegmento is null
    group by 2, 3, 4, 5, 6
) f
where not (segmento = 18 and qnt_folhas = 0) and segmento <> 15
group by 1, 2, 3, 4, 5;