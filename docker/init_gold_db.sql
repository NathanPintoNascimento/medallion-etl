-- =============================================================
-- Gold Layer — Schema analítico (PostgreSQL)
-- Dataset: Transferências de Renda / Programas Sociais
-- =============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Agregação mensal por UF
CREATE TABLE IF NOT EXISTS gold.transferencias_por_uf_mes (
    id               SERIAL PRIMARY KEY,
    ano              INTEGER       NOT NULL,
    mes              INTEGER       NOT NULL,
    uf               VARCHAR(2)    NOT NULL,
    total_beneficios BIGINT        NOT NULL,
    total_valor_brl  NUMERIC(18,2) NOT NULL,
    media_valor_brl  NUMERIC(12,2),
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Ranking por município
CREATE TABLE IF NOT EXISTS gold.top_municipios_beneficios (
    id               SERIAL PRIMARY KEY,
    ano              INTEGER       NOT NULL,
    municipio        VARCHAR(200)  NOT NULL,
    uf               VARCHAR(2)    NOT NULL,
    codigo_ibge      VARCHAR(10),
    total_beneficios BIGINT        NOT NULL,
    total_valor_brl  NUMERIC(18,2) NOT NULL,
    ranking_nacional INTEGER,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Série temporal mensal (visão geral Brasil)
CREATE TABLE IF NOT EXISTS gold.serie_temporal_brasil (
    id               SERIAL PRIMARY KEY,
    ano_mes          VARCHAR(7)    NOT NULL UNIQUE,  -- YYYY-MM
    total_beneficios BIGINT        NOT NULL,
    total_valor_brl  NUMERIC(18,2) NOT NULL,
    variacao_pct     NUMERIC(8,4),
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_uf_mes       ON gold.transferencias_por_uf_mes(uf, ano, mes);
CREATE INDEX IF NOT EXISTS idx_top_mun_ano  ON gold.top_municipios_beneficios(ano, ranking_nacional);
CREATE INDEX IF NOT EXISTS idx_serie_anomes ON gold.serie_temporal_brasil(ano_mes);
