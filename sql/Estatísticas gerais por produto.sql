-- Estatísticas gerais por produto
SELECT 
    produto,
    COUNT(*) as total_registros,
    ROUND(MIN(valor_producao_mil_reais)::numeric, 2) as valor_min_mil_reais,
    ROUND(MAX(valor_producao_mil_reais)::numeric, 2) as valor_max_mil_reais,
    ROUND(AVG(valor_producao_mil_reais)::numeric, 2) as valor_medio_mil_reais,
    ROUND(MIN(rendimento_medio_kg_ha)::numeric, 2) as rendimento_min_kg_ha,
    ROUND(MAX(rendimento_medio_kg_ha)::numeric, 2) as rendimento_max_kg_ha,
    ROUND(AVG(rendimento_medio_kg_ha)::numeric, 2) as rendimento_medio_kg_ha
FROM processed.producao_agricola
WHERE status_dado = 'completo'
GROUP BY produto
ORDER BY valor_medio_mil_reais DESC;