

WITH source_setor
    AS (
        SELECT
            NULLIF("CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF("CD_FATOR", 'NaN') AS "CD_FATOR"
            , NULLIF("CD_GRUPO_DE_CUSTO", 'NaN') AS "CD_GRUPO_DE_CUSTO"
            , NULLIF(SPLIT_PART("CD_SETOR_CUSTO", '.', 1), 'NaN') AS "CD_SETOR_CUSTO"
            , NULLIF("NM_SETOR", 'NaN') AS "NM_SETOR"
            , NULLIF("SN_ATIVO", 'NaN') AS "SN_ATIVO"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'setor') }}
),
treats
    AS (
        SELECT
            "CD_SETOR"::BIGINT
            , "CD_FATOR"::BIGINT
            , "CD_GRUPO_DE_CUSTO"::BIGINT
            , "CD_SETOR_CUSTO"::BIGINT
            , "NM_SETOR"::VARCHAR(70)
            , "SN_ATIVO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_setor
    )
SELECT * FROM treats