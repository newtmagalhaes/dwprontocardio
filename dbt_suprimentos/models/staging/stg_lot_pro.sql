

WITH source_lot_pro 
    AS (
        SELECT
            NULLIF("CD_LOT_PRO", 'NaN') AS "CD_LOT_PRO"
            , NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_LOTE", 'NaN') AS "CD_LOTE"
            , "DT_VALIDADE"
            , NULLIF("QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'lot_pro') }}
),
treats 
    AS (
        SELECT
            "CD_LOT_PRO"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_LOTE"::VARCHAR(555)
            , "DT_VALIDADE"::DATE
            , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_lot_pro
    )
SELECT * FROM treats