

WITH source_estoque 
    AS (
        SELECT
              NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF("CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF("DS_ESTOQUE", 'NaN') AS "DS_ESTOQUE"
            , NULLIF("TP_ESTOQUE", 'NaN') AS "TP_ESTOQUE"
            , "DT_EXTRACAO" AS "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'estoque') }}
),
treats 
    AS (
        SELECT
            "CD_ESTOQUE"::BIGINT
            , "CD_SETOR"::BIGINT
            , "DS_ESTOQUE"::VARCHAR(30)
            , "TP_ESTOQUE"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_estoque
)
SELECT * FROM treats