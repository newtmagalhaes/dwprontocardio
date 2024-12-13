

WITH source_mot_cancel 
    AS (
        SELECT
            NULLIF("CD_MOT_CANCEL", 'NaN') AS "CD_MOT_CANCEL"
            , NULLIF("DS_MOT_CANCEL", 'NaN') AS "DS_MOT_CANCEL"
            , NULLIF("TP_MOT_CANCEL", 'NaN') AS "TP_MOT_CANCEL"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'mot_cancel') }}
),
treats
    AS (
        SELECT
            "CD_MOT_CANCEL"::BIGINT
            , "DS_MOT_CANCEL"::VARCHAR(60)
            , "TP_MOT_CANCEL"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_mot_cancel   
    )
SELECT * FROM treats