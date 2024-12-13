

WITH source_fornecedor 
    AS (
        SELECT
            NULLIF("CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"
            , NULLIF("NM_FORNECEDOR", 'NaN') AS "NM_FORNECEDOR"
            , NULLIF("NM_FANTASIA", 'NaN') AS "NM_FANTASIA"
            , "DT_INCLUSAO"
            , NULLIF("NR_CGC_CPF", 'NaN') AS "NR_CGC_CPF"
            , NULLIF("TP_FORNECEDOR", 'NaN') AS "TP_FORNECEDOR"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'fornecedor') }}
),
treats 
    AS (
        SELECT
            "CD_FORNECEDOR"::BIGINT
            , "NM_FORNECEDOR"::VARCHAR(250)
            , "NM_FANTASIA"::VARCHAR(100)
            , "DT_INCLUSAO"::DATE
            , "NR_CGC_CPF"::NUMERIC(14,0)
            , "TP_FORNECEDOR"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_fornecedor
)
SELECT * FROM treats