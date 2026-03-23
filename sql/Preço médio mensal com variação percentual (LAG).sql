-- Preço médio anual por commodity com variação percentual
WITH preco_anual AS (
    SELECT 
        produto,
        ano,
        AVG(CASE 
            WHEN qtd_produzida > 0 THEN (valor_producao_mil_reais * 1000) / qtd_produzida 
            ELSE NULL 
        END) as preco_medio_reais_por_tonelada
    FROM processed.producao_agricola
    WHERE qtd_produzida > 0 
      AND valor_producao_mil_reais > 0
    GROUP BY produto, ano
),
preco_com_lag AS (
    SELECT 
        produto,
        ano,
        preco_medio_reais_por_tonelada,
        LAG(preco_medio_reais_por_tonelada) OVER (
            PARTITION BY produto 
            ORDER BY ano
        ) as preco_ano_anterior
    FROM preco_anual
)
SELECT 
    produto,
    ano,
    ROUND(preco_medio_reais_por_tonelada::numeric, 4) as preco_medio_reais_ton,
    ROUND(preco_ano_anterior::numeric, 4) as preco_ano_anterior_reais_ton,
    CASE 
        WHEN preco_ano_anterior > 0 THEN 
            ROUND(((preco_medio_reais_por_tonelada - preco_ano_anterior) / preco_ano_anterior * 100)::numeric, 2)
        ELSE NULL 
    END as variacao_percentual
FROM preco_com_lag
WHERE preco_ano_anterior IS NOT NULL
ORDER BY produto, ano DESC;