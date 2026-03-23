-- ============================================================
--  verificar_processed.sql
--  Execute no DBeaver conectado ao banco agro_dw
-- ============================================================


-- 1. Total de registros na camada processed
SELECT COUNT(*) AS total_registros
FROM processed.producao_agricola;


-- 2. Todos os dados tratados
SELECT
    produto,
    produto_nome_ibge,
    ano,
    area_colhida_ha,
    qtd_produzida,
    unidade_qtd,
    rendimento_medio_kg_ha,
    valor_producao_mil_reais,
    status_dado,
    processado_em
FROM processed.producao_agricola
ORDER BY ano DESC, valor_producao_mil_reais DESC NULLS LAST;


-- 3. Registros parciais (dados ausentes)
SELECT
    produto,
    ano,
    area_colhida_ha,
    qtd_produzida,
    rendimento_medio_kg_ha,
    valor_producao_mil_reais,
    status_dado
FROM processed.producao_agricola
WHERE status_dado = 'parcial'
ORDER BY ano DESC;


-- 4. Produtos padronizados vs nome original IBGE
SELECT DISTINCT
    produto           AS nome_padronizado,
    produto_nome_ibge AS nome_original_ibge,
    produto_cod
FROM processed.producao_agricola
ORDER BY produto;