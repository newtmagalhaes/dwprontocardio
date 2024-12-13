

WITH source_itsol_com 
    AS (
        SELECT
            NULLIF("CD_SOL_COM", 'NaN') AS "CD_SOL_COM"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF("CD_MOT_CANCEL", 'NaN') AS "CD_MOT_CANCEL"
            , "DT_CANCEL"
            , NULLIF("QT_SOLIC", 'NaN') AS "QT_SOLIC"
            , NULLIF("QT_COMPRADA", 'NaN') AS "QT_COMPRADA"
            , NULLIF("QT_ATENDIDA", 'NaN') AS "QT_ATENDIDA"
            , NULLIF("SN_COMPRADO", 'NaN') AS "SN_COMPRADO"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itsol_com') }}
),
treats 
    AS (
        SELECT
            "CD_SOL_COM"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_MOT_CANCEL"::BIGINT
            , "DT_CANCEL"::TIMESTAMP
            , "QT_SOLIC"::NUMERIC(11,3)
            , "QT_COMPRADA"::NUMERIC(11,3)
            , "QT_ATENDIDA"::NUMERIC(11,3)
            , "SN_COMPRADO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_itsol_com
    )
SELECT * FROM treats