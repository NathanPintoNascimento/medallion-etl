-- ═══════════════════════════════════════════════════════════════════════════
-- queries_gold.sql — Consultas analíticas na camada Gold (PostgreSQL)
-- Dataset: Bolsa Família — dados.gov.br
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Visão geral: total nacional por mês
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ano_mes,
    total_beneficios,
    total_valor_brl,
    ROUND(total_valor_brl / 1e9, 3)  AS total_bilhoes_brl,
    ROUND(variacao_pct, 2)           AS variacao_pct
FROM gold.serie_temporal_brasil
ORDER BY ano_mes;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Ranking de UFs: maior número de beneficiários (último mês disponível)
-- ─────────────────────────────────────────────────────────────────────────────
WITH ultimo_mes AS (
    SELECT MAX(ano * 100 + mes) AS max_periodo FROM gold.transferencias_por_uf_mes
)
SELECT
    t.uf,
    t.total_beneficios,
    t.total_valor_brl,
    t.media_valor_brl,
    RANK() OVER (ORDER BY t.total_beneficios DESC) AS ranking
FROM gold.transferencias_por_uf_mes t
JOIN ultimo_mes u ON (t.ano * 100 + t.mes) = u.max_periodo
ORDER BY ranking;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Top 20 municípios com mais beneficiários
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ranking_nacional,
    municipio,
    uf,
    total_beneficios,
    ROUND(total_valor_brl / 1e6, 2) AS valor_milhoes_brl
FROM gold.top_municipios_beneficios
WHERE ranking_nacional <= 20
ORDER BY ranking_nacional;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Comparação entre regiões (Norte, Nordeste, Sul, Sudeste, Centro-Oeste)
-- ─────────────────────────────────────────────────────────────────────────────
WITH regioes AS (
    SELECT uf,
        CASE
            WHEN uf IN ('AC','AM','AP','PA','RO','RR','TO') THEN 'Norte'
            WHEN uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')  THEN 'Nordeste'
            WHEN uf IN ('ES','MG','RJ','SP')                           THEN 'Sudeste'
            WHEN uf IN ('PR','RS','SC')                                THEN 'Sul'
            WHEN uf IN ('DF','GO','MS','MT')                           THEN 'Centro-Oeste'
            ELSE 'Outros'
        END AS regiao
    FROM (SELECT DISTINCT uf FROM gold.transferencias_por_uf_mes) t
)
SELECT
    r.regiao,
    SUM(t.total_beneficios) AS total_beneficios,
    ROUND(SUM(t.total_valor_brl), 2) AS total_valor_brl,
    ROUND(AVG(t.media_valor_brl), 2) AS media_valor_brl
FROM gold.transferencias_por_uf_mes t
JOIN regioes r ON t.uf = r.uf
GROUP BY r.regiao
ORDER BY total_beneficios DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Mês com maior crescimento percentual
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ano_mes,
    total_beneficios,
    variacao_pct,
    CASE
        WHEN variacao_pct > 5  THEN '🟢 Crescimento expressivo'
        WHEN variacao_pct > 0  THEN '🔵 Crescimento leve'
        WHEN variacao_pct = 0  THEN '⚪ Estável'
        WHEN variacao_pct < 0  THEN '🔴 Queda'
        ELSE '⬜ Sem dados'
    END AS tendencia
FROM gold.serie_temporal_brasil
WHERE variacao_pct IS NOT NULL
ORDER BY variacao_pct DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Ticket médio por UF (ordenado pelo maior valor médio por beneficiário)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    uf,
    ROUND(AVG(media_valor_brl), 2)       AS ticket_medio_brl,
    SUM(total_beneficios)                AS total_beneficiarios,
    ROUND(SUM(total_valor_brl) / 1e6, 2) AS total_milhoes_brl
FROM gold.transferencias_por_uf_mes
GROUP BY uf
ORDER BY ticket_medio_brl DESC;
