

WITH source_produto 
    AS (
        SELECT
            NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_ESPECIE", 'NaN') AS "CD_ESPECIE"
            , NULLIF("DS_PRODUTO", 'NaN') AS "DS_PRODUTO"
            , NULLIF("DS_PRODUTO_RESUMIDO", 'NaN') AS "DS_PRODUTO_RESUMIDO"
            , "DT_CADASTRO"
            , "DT_ULTIMA_ENTRADA"
            , "HR_ULTIMA_ENTRADA"
            , NULLIF("QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , NULLIF("QT_ULTIMA_ENTRADA", 'NaN') AS "QT_ULTIMA_ENTRADA"
            , NULLIF("VL_ULTIMA_ENTRADA", 'NaN') AS "VL_ULTIMA_ENTRADA"
            , NULLIF("VL_CUSTO_MEDIO", 'NaN') AS "VL_CUSTO_MEDIO"
            , NULLIF("VL_ULTIMA_CUSTO_REAL", 'NaN') AS "VL_ULTIMA_CUSTO_REAL"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'produto') }}
),
treats
    AS (
        SELECT
            "CD_PRODUTO"::BIGINT
            , "CD_ESPECIE"::BIGINT
            , "DS_PRODUTO"::VARCHAR(60)
            , "DS_PRODUTO_RESUMIDO"::VARCHAR(40)
            , "DT_CADASTRO"::TIMESTAMP
            , "DT_ULTIMA_ENTRADA"::DATE
            , "HR_ULTIMA_ENTRADA"::TIMESTAMP
            , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
            , "QT_ULTIMA_ENTRADA"::NUMERIC(11,3)
            , "VL_ULTIMA_ENTRADA"::NUMERIC(12,2)
            , "VL_CUSTO_MEDIO"::NUMERIC(12,2)
            , "VL_ULTIMA_CUSTO_REAL"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_produto
    )
SELECT * FROM treats