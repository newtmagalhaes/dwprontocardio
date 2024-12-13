



WITH source_especie
    AS (
        SELECT
            NULLIF("CD_ESPECIE", 'NaN') AS "CD_ESPECIE"
            , NULLIF(SPLIT_PART("CD_ITEM_RES", '.', 1), 'NaN') AS "CD_ITEM_RES"
            , NULLIF("DS_ESPECIE", 'NaN') AS "DS_ESPECIE"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'especie') }}
),
treats
    AS (
        SELECT
            "CD_ESPECIE"::BIGINT
            , "CD_ITEM_RES"::BIGINT
            , "DS_ESPECIE"::VARCHAR(240)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_especie
    )
SELECT * FROM treats