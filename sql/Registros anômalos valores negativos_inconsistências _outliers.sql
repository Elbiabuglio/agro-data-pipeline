-- =====================================================
-- DETECÇÃO DE ANOMALIAS NA TABELA FATO_PRODUCAO
-- =====================================================

-- 1. REGISTROS COM VALORES NEGATIVOS
-- Identifica registros com valores negativos em campos que não deveriam ter valores negativos
-- Área colhida, quantidade produzida e valor da produção devem ser sempre positivos
SELECT 'VALORES_NEGATIVOS' AS tipo_anomalia, *
FROM public.fato_producao
WHERE area_colhida_ha < 0 
   OR qtd_produzida < 0 
   OR valor_producao_mil_reais < 0;

-- 2. VALORES ABSURDAMENTE ALTOS (OUTLIERS EXTREMOS)
-- Detecta registros com valores extremamente altos que podem indicar erros de entrada de dados
-- Define limites máximos plausíveis para área, produção e valor
SELECT 'VALORES_EXTREMOS' AS tipo_anomalia, *
FROM public.fato_producao
WHERE area_colhida_ha > 10000000        -- área maior que 10 milhões de hectares (implausível)
   OR qtd_produzida > 100000000         -- produção maior que 100 milhões de toneladas
   OR valor_producao_mil_reais > 1000000000; -- valor maior que 1 trilhão de reais (em milhares)

-- 3. OUTLIERS ESTATÍSTICOS (TOP 5% DOS VALORES)
-- Identifica os 5% maiores valores de produção usando percentil 95
-- Útil para detectar outliers estatísticos que podem ser válidos mas merecem atenção
WITH limites AS (
    SELECT 
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY valor_producao_mil_reais) AS p95
    FROM public.fato_producao
)
SELECT 'OUTLIERS_ESTATISTICOS' AS tipo_anomalia, f.*
FROM public.fato_producao f, limites l
WHERE f.valor_producao_mil_reais > l.p95;

-- 4. RENDIMENTO IMPOSSÍVEL (PRODUTIVIDADE EXTREMA)
-- Calcula o rendimento por hectare e identifica valores impossíveis
-- Rendimento = (Produção em kg) / Área colhida
-- Valores acima de 100 ton/ha são suspeitos para a maioria das culturas
SELECT 'RENDIMENTO_IMPOSSIVEL' AS tipo_anomalia, 
       *,
       (qtd_produzida * 1000 / NULLIF(area_colhida_ha, 0)) AS rendimento_kg_por_ha
FROM public.fato_producao
WHERE (qtd_produzida * 1000 / NULLIF(area_colhida_ha, 0)) > 100000;

-- =====================================================
-- RESUMO DOS TIPOS DE ANOMALIAS DETECTADAS:
-- - VALORES_NEGATIVOS: Erros óbvios de entrada de dados
-- - VALORES_EXTREMOS: Números implausíveis que indicam erros grosseiros  
-- - OUTLIERS_ESTATISTICOS: Valores altos mas possivelmente válidos
-- - RENDIMENTO_IMPOSSIVEL: Produtividade fisicamente impossível
-- =====================================================