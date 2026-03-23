-- Resumo de anomalias por categoria
SELECT 
    CASE 
        WHEN area_colhida_ha <= 0 THEN 'ÁREA INVÁLIDA'
        WHEN qtd_produzida <= 0 THEN 'PRODUÇÃO INVÁLIDA'
        WHEN valor_producao_mil_reais <= 0 THEN 'VALOR INVÁLIDO'
        WHEN rendimento_medio_kg_ha <= 0 THEN 'RENDIMENTO INVÁLIDO'
        ELSE 'DADOS VÁLIDOS'
    END as categoria,
    COUNT(*) as total_registros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentual
FROM processed.producao_agricola
GROUP BY 1
ORDER BY total_registros DESC;