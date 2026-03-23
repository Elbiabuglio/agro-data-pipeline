-- Comparação de preços entre 2023 e 2024
SELECT 
    produto,
    ROUND(AVG(CASE WHEN ano = 2023 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END)::numeric, 4) as preco_2023,
    ROUND(AVG(CASE WHEN ano = 2024 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END)::numeric, 4) as preco_2024,
    ROUND(((AVG(CASE WHEN ano = 2024 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END) - 
            AVG(CASE WHEN ano = 2023 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END)) / 
           AVG(CASE WHEN ano = 2023 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END) * 100)::numeric, 2) as variacao_percentual
FROM processed.producao_agricola
WHERE qtd_produzida > 0 
  AND valor_producao_mil_reais > 0
  AND ano IN (2023, 2024)
GROUP BY produto
HAVING AVG(CASE WHEN ano = 2023 THEN (valor_producao_mil_reais * 1000) / qtd_produzida END) > 0
ORDER BY variacao_percentual DESC;