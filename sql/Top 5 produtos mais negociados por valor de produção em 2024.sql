-- Top 5 produtos mais negociados por valor de produção em 2024
SELECT 
    produto,
    COUNT(*) as total_registros,
    SUM(area_colhida_ha) as area_total_ha,
    SUM(qtd_produzida) as producao_total_toneladas,
    SUM(valor_producao_mil_reais) as valor_total_mil_reais,
    ROUND(AVG(rendimento_medio_kg_ha)::numeric, 2) as rendimento_medio_kg_ha,
    ROUND((SUM(valor_producao_mil_reais * 1000) / SUM(qtd_produzida))::numeric, 4) as preco_medio_reais_ton,
    ROW_NUMBER() OVER (ORDER BY SUM(valor_producao_mil_reais) DESC) as ranking_valor
FROM processed.producao_agricola
WHERE ano = 2024
  AND status_dado = 'completo'
  AND qtd_produzida > 0
GROUP BY produto
ORDER BY valor_total_mil_reais DESC
LIMIT 5;