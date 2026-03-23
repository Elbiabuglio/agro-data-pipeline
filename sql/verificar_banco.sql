-- ============================================================
--  verificar_banco.sql
--  Verifica estrutura e dados do banco agro_dw
--  Execute no DBeaver conectado ao banco agro_dw
-- ============================================================


-- ── 1. Lista todos os bancos disponíveis ────────────────────
SELECT datname AS banco
FROM pg_database
ORDER BY datname;


-- ── 2. Lista as tabelas do schema public ────────────────────
SELECT
    table_name      AS tabela,
    table_type      AS tipo
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;


-- ── 3. Estrutura de cada tabela ─────────────────────────────
SELECT
    table_name      AS tabela,
    column_name     AS coluna,
    data_type       AS tipo,
    is_nullable     AS nulo,
    column_default  AS valor_padrao
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('dim_commodity', 'dim_territorio', 'fato_producao')
ORDER BY table_name, ordinal_position;


-- ── 4. Chaves primárias e estrangeiras ──────────────────────
SELECT
    tc.table_name           AS tabela,
    tc.constraint_name      AS constraint,
    tc.constraint_type      AS tipo,
    kcu.column_name         AS coluna,
    ccu.table_name          AS tabela_referenciada,
    ccu.column_name         AS coluna_referenciada
FROM information_schema.table_constraints        tc
JOIN information_schema.key_column_usage         kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
   AND tc.table_schema    = ccu.table_schema
WHERE tc.table_schema = 'public'
  AND tc.table_name   IN ('dim_commodity', 'dim_territorio', 'fato_producao')
  AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
ORDER BY tc.table_name, tc.constraint_type;


-- ── 5. Contagem de registros por tabela ─────────────────────
SELECT 'dim_commodity'  AS tabela, COUNT(*) AS total FROM dim_commodity
UNION ALL
SELECT 'dim_territorio' AS tabela, COUNT(*) AS total FROM dim_territorio
UNION ALL
SELECT 'fato_producao'  AS tabela, COUNT(*) AS total FROM fato_producao
ORDER BY tabela;


-- ── 6. Conteúdo completo — dim_commodity ────────────────────
SELECT *
FROM dim_commodity
ORDER BY id_commodity;


-- ── 7. Conteúdo completo — dim_territorio ───────────────────
SELECT *
FROM dim_territorio
ORDER BY id_territorio;


-- ── 8. Conteúdo completo — fato_producao ────────────────────
SELECT
    f.id_producao,
    c.nome_oficial                  AS produto,
    t.nome                          AS territorio,
    f.ano,
    f.area_colhida_ha,
    f.qtd_produzida,
    f.rendimento_medio_kg_ha,
    f.valor_producao_mil_reais,
    f.coletado_em
FROM fato_producao       f
JOIN dim_commodity  c ON c.id_commodity  = f.id_commodity
JOIN dim_territorio t ON t.id_territorio = f.id_territorio
ORDER BY f.ano DESC, f.valor_producao_mil_reais DESC NULLS LAST;


-- ── 9. Top 5 commodities por valor de produção (2023) ───────
SELECT
    c.nome_oficial                  AS produto,
    f.ano,
    f.qtd_produzida,
    f.valor_producao_mil_reais
FROM fato_producao       f
JOIN dim_commodity  c ON c.id_commodity  = f.id_commodity
WHERE f.ano = 2023
  AND f.valor_producao_mil_reais IS NOT NULL
ORDER BY f.valor_producao_mil_reais DESC
LIMIT 5;


-- ── 10. Verifica integridade referencial (FKs órfãs) ────────
SELECT
    'fato → dim_commodity'  AS verificacao,
    COUNT(*)                AS orfas
FROM fato_producao
WHERE id_commodity NOT IN (SELECT id_commodity FROM dim_commodity)

UNION ALL

SELECT
    'fato → dim_territorio' AS verificacao,
    COUNT(*)                AS orfas
FROM fato_producao
WHERE id_territorio NOT IN (SELECT id_territorio FROM dim_territorio);