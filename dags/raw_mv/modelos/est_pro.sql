

SELECT
    ep.CD_ESTOQUE
    , ep.CD_PRODUTO
    , ep.CD_LOCALIZACAO
    , ep.DT_ULTIMA_MOVIMENTACAO
    , ep.QT_ESTOQUE_ATUAL
    , ep.QT_ESTOQUE_MAXIMO
    , ep.QT_ESTOQUE_MINIMO
    , ep.QT_ESTOQUE_VIRTUAL
    , ep.QT_PONTO_DE_PEDIDO
    , ep.QT_CONSUMO_MES
    , ep.QT_SOLICITACAO_DE_COMPRA
    , ep.QT_ORDEM_DE_COMPRA
    , ep.QT_ESTOQUE_DOADO
    , ep.QT_ESTOQUE_RESERVADO
    , ep.QT_CONSUMO_ATUAL
    , ep.DS_LOCALIZACAO_PRATELEIRA
    , ep.TP_CLASSIFICACAO_ABC
FROM DBAMV.EST_PRO ep

