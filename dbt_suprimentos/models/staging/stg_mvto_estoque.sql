

WITH source_mvto_estoque
    AS (
        SELECT
            NULLIF("CD_MVTO_ESTOQUE", 'NaN') AS "CD_MVTO_ESTOQUE"
            , NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF("CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(SPLIT_PART("CD_UNID_INT", '.', 1), 'NaN') AS "CD_UNID_INT"
            , NULLIF(SPLIT_PART("CD_SETOR", '.', 1), 'NaN') AS "CD_SETOR"
            , NULLIF(SPLIT_PART("CD_ESTOQUE_DESTINO", '.', 1), 'NaN') AS "CD_ESTOQUE_DESTINO"
            , NULLIF("CD_CUSTO_MEDIO", 'NaN') AS "CD_CUSTO_MEDIO"
            , NULLIF(SPLIT_PART("CD_AVISO_CIRURGIA", '.', 1), 'NaN') AS "CD_AVISO_CIRURGIA"
            , NULLIF(SPLIT_PART("CD_ENT_PRO", '.', 1), 'NaN') AS "CD_ENT_PRO"
            , NULLIF("CD_USUARIO", 'NaN') AS "CD_USUARIO"
            , NULLIF(SPLIT_PART("CD_FORNECEDOR", '.', 1), 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(SPLIT_PART("CD_PRESTADOR", '.', 1), 'NaN') AS "CD_PRESTADOR"
            , NULLIF(SPLIT_PART("CD_PRE_MED", '.', 1), 'NaN') AS "CD_PRE_MED"
            , NULLIF(SPLIT_PART("CD_ATENDIMENTO", '.', 1), 'NaN') AS "CD_ATENDIMENTO"
            , NULLIF(SPLIT_PART("CD_MOT_DEV", '.', 1), 'NaN') AS "CD_MOT_DEV"
            , "DT_MVTO_ESTOQUE"
            , "HR_MVTO_ESTOQUE"
            , NULLIF("VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF("TP_MVTO_ESTOQUE", 'NaN') AS "TP_MVTO_ESTOQUE"
            , NULLIF("NR_DOCUMENTO", 'NaN') AS "NR_DOCUMENTO"
            , NULLIF("CHAVE_NFE", 'NaN') AS "CHAVE_NFE"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'mvto_estoque') }}
),
treats
    AS (
        SELECT
            "CD_MVTO_ESTOQUE"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_UNID_INT"::BIGINT
            , "CD_SETOR"::BIGINT
            , "CD_ESTOQUE_DESTINO"::BIGINT
            , "CD_CUSTO_MEDIO"::BIGINT
            , "CD_AVISO_CIRURGIA"::BIGINT
            , "CD_ENT_PRO"::BIGINT
            , "CD_USUARIO"::VARCHAR(50)
            , "CD_FORNECEDOR"::BIGINT
            , "CD_PRESTADOR"::BIGINT
            , "CD_PRE_MED"::BIGINT
            , "CD_ATENDIMENTO"::BIGINT
            , "CD_MOT_DEV"::BIGINT
            , "DT_MVTO_ESTOQUE"::DATE
            , "HR_MVTO_ESTOQUE"::TIMESTAMP
            , "VL_TOTAL"::NUMERIC(15,0)
            , "TP_MVTO_ESTOQUE"::VARCHAR(1)
            , "NR_DOCUMENTO"::VARCHAR(20)
            , "CHAVE_NFE"::VARCHAR(44)
        FROM source_mvto_estoque
    )
SELECT * FROM treats