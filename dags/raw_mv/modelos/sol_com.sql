SELECT
    sc.CD_SOL_COM
    , sc.CD_MOT_PED
    , sc.CD_SETOR
    , sc.CD_ESTOQUE
    , sc.CD_MOT_CANCEL
    , sc.CD_ATENDIME
    , sc.VL_TOTAL
    , sc.DT_SOL_COM
    , sc.DT_CANCELAMENTO
    , sc.NM_SOLICITANTE
    , sc.TP_SITUACAO
    , sc.TP_SOL_COM
    , sc.SN_URGENTE
    , sc.SN_APROVADA
    , sc.SN_OPME
FROM DBAMV.SOL_COM sc
WHERE sc.CD_SOL_COM > :MAIOR_ID
