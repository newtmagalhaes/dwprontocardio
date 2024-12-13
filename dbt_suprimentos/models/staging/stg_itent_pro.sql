

WITH source_itent_pro 
    AS (
        SELECT
            NULLIF(SPLIT_PART("CD_ITENT_PRO"::TEXT, '.', 1), 'NaN') AS "CD_ITENT_PRO"
            , NULLIF("CD_ENT_PRO", 'NaN') AS "CD_ENT_PRO"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF("CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO"
            , NULLIF("CD_CUSTO_MEDIO", 'NaN') AS "CD_CUSTO_MEDIO"
            , NULLIF("CD_PRODUTO_FORNECEDOR", 'NaN') AS "CD_PRODUTO_FORNECEDOR"
            , "DT_GRAVACAO"
            , NULLIF("QT_ENTRADA", 'NaN') AS "QT_ENTRADA"
            , NULLIF("QT_DEVOLVIDA", 'NaN') AS "QT_DEVOLVIDA"
            , NULLIF("QT_ATENDIDA", 'NaN') AS "QT_ATENDIDA"
            , NULLIF("VL_UNITARIO", 'NaN') AS "VL_UNITARIO"
            , NULLIF("VL_CUSTO_REAL", 'NaN') AS "VL_CUSTO_REAL"
            , NULLIF("VL_TOTAL_CUSTO_REAL", 'NaN') AS "VL_TOTAL_CUSTO_REAL"
            , NULLIF("VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itent_pro') }}
),
treats
    AS (
        SELECT
            "CD_ITENT_PRO"::BIGINT
            , "CD_ENT_PRO"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_ATENDIMENTO"::BIGINT
            , "CD_CUSTO_MEDIO"::BIGINT
            , "CD_PRODUTO_FORNECEDOR"::BIGINT
            , "DT_GRAVACAO"::TIMESTAMP
            , "QT_ENTRADA"::NUMERIC(11,3)
            , "QT_DEVOLVIDA"::NUMERIC(11,3)
            , "QT_ATENDIDA"::NUMERIC(11,3)
            , "VL_UNITARIO"::NUMERIC(12,2)
            , "VL_CUSTO_REAL"::NUMERIC(12,2)
            , "VL_TOTAL_CUSTO_REAL"::NUMERIC(12,2)
            , "VL_TOTAL"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_itent_pro
    )
SELECT * FROM treats