#  Pipeline ETL com Arquitetura Medallion

> Pipeline de Engenharia de Dados completo usando *Python · PySpark · Delta Lake · Apache Airflow*  
> Dataset: *Pagamentos do Bolsa Família* — [dados.gov.br](https://dados.gov.br) / Portal da Transparência

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange?logo=apache-spark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-00ADD8)](https://delta.io)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

##  Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Estrutura de Pastas](#-estrutura-de-pastas)
- [Dataset](#-dataset)
- [Camadas Medallion](#-camadas-medallion)
- [Como Rodar](#-como-rodar)
- [Testes](#-testes)
- [Airflow DAG](#-airflow-dag)
- [Consultas SQL Gold](#-consultas-sql-gold)
- [Stack Técnica](#-stack-técnica)

---

##  Visão Geral

Este projeto implementa um *pipeline ETL de ponta a ponta* com a arquitetura *Medallion* (Bronze → Silver → Gold), processando dados reais do Portal de Dados Abertos do Governo Brasileiro.

*O que o pipeline faz:*

```
Portal da Transparência (dados.gov.br)
         │
         ▼
  [Bronze] Ingestão raw (CSV → Delta Lake)
         │
         ▼
  [Silver] Limpeza, tipagem, deduplicação (Delta Lake)
         │
         ▼
  [Gold]  Agregações analíticas (Delta Lake + PostgreSQL)
         │
         ▼
  Dashboard / BI / Jupyter Notebook
```

---

##  Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PIPELINE ETL MEDALLION                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                   ORQUESTRAÇÃO (Apache Airflow)                   │  │
│  │                                                                   │  │
│  │   [start] ──► [bronze_ingestion] ──► [silver_transform]          │  │
│  │                                            │                     │  │
│  │                              [gold_aggregations] ──► [data_quality_check] ──► [end]  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────────┐   │
│  │   🟫 BRONZE  │   │   🥈 SILVER  │   │       🥇 GOLD            │   │
│  │              │   │              │   │                          │   │
│  │  Raw CSV     │   │  Tipagem     │   │  transferencias_por_uf   │   │
│  │  sem filtros │──►│  Limpeza     │──►│  top_municipios          │   │
│  │  Metadados   │   │  Dedup       │   │  serie_temporal_brasil   │   │
│  │  de auditoria│   │  Normalização│   │                          │   │
│  │              │   │              │   │  ┌──────────────────┐    │   │
│  │  Delta Lake  │   │  Delta Lake  │   │  │   PostgreSQL      │    │   │
│  │  Part: ano/  │   │  Part: ano/  │   │  │   schema: gold    │    │   │
│  │        mes   │   │        mes   │   │  └──────────────────┘    │   │
│  └──────────────┘   └──────────────┘   └──────────────────────────┘   │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │                    INFRAESTRUTURA                               │   │
│  │   Docker Compose · PostgreSQL · Jupyter Lab · PySpark Local    │   │
│  └────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

FONTE DE DADOS
─────────────
  dados.gov.br ──► Portal da Transparência (CSV mensal, ~200MB/mês)
  Fallback      ──► Gerador de dados sintéticos (50k linhas/mês)
```

### Fluxo de dados detalhado

```
dados.gov.br/Portal Transparência
    │
    │  HTTP GET  (CSV ; Latin-1 ; ~200MB)
    ▼
┌───────────────────────────────────────────────────────────────────┐
│  BRONZE — bronze_ingestion.py                                     │
│                                                                   │
│  Input:  CSV raw (8 colunas, todos String)                        │
│  Output: Delta Lake particionado por ano/mes                      │
│                                                                   │
│  Colunas adicionadas:                                             │
│    _source       → URL ou "sample_data"                           │
│    _ingested_at  → timestamp UTC da ingestão                      │
│    _file_hash    → MD5 do arquivo para rastreabilidade            │
│    ano, mes      → partição explícita                             │
└───────────────────────────────────────────────────────────────────┘
    │
    │  spark.read.format("delta")
    ▼
┌───────────────────────────────────────────────────────────────────┐
│  SILVER — silver_transform.py                                     │
│                                                                   │
│  Transformações aplicadas (em ordem):                             │
│  1. Rename: MES_COMPETENCIA → competencia, etc.                   │
│  2. Tipagem: VALOR_PARCELA "218,00" → 218.0 (Double)             │
│  3. Extração: competencia "202301" → ano=2023, mes=1             │
│  4. Normalização: upper() + trim() em strings                     │
│  5. Filtros: UFs válidas, valor > 0                               │
│  6. Drop nulls: nis_favorecido, uf, valor_parcela                 │
│  7. Dedup: window por (nis + ano + mes), mantém maior valor       │
│  8. Auditoria: dt_ingestao, _silver_version                       │
└───────────────────────────────────────────────────────────────────┘
    │
    │  spark.read.format("delta")
    ▼
┌───────────────────────────────────────────────────────────────────┐
│  GOLD — gold_aggregations.py                                      │
│                                                                   │
│  Tabela 1: transferencias_por_uf_mes                              │
│    GROUP BY ano, mes, uf                                          │
│    → count, sum(valor), avg(valor)                                │
│                                                                   │
│  Tabela 2: top_municipios_beneficios                              │
│    GROUP BY ano, municipio, uf                                    │
│    → count, sum(valor), ranking_nacional                          │
│                                                                   │
│  Tabela 3: serie_temporal_brasil                                  │
│    GROUP BY ano_mes                                               │
│    → count, sum(valor), variacao_pct (LAG window)                 │
│                                                                   │
│  Saída: Delta Lake (local) + PostgreSQL (schema gold)             │
└───────────────────────────────────────────────────────────────────┘
```

---

##  Estrutura de Pastas

```
medallion-etl/
│
├── dags/                          # DAGs do Apache Airflow
│   └── dag_medallion_pipeline.py  # DAG principal: Bronze → Silver → Gold
│
├── src/                           # Código-fonte do pipeline
│   ├── bronze/
│   │   └── bronze_ingestion.py    # Ingestão raw (dados.gov.br → Delta Lake)
│   ├── silver/
│   │   └── silver_transform.py    # Limpeza, tipagem, deduplicação
│   ├── gold/
│   │   └── gold_aggregations.py   # Agregações analíticas
│   └── utils/
│       ├── config.py              # Configurações centrais (paths, DB, Spark)
│       ├── spark_session.py       # Factory da SparkSession com Delta Lake
│       ├── logger.py              # Logger padronizado
│       └── sample_data.py         # Gerador de dados sintéticos (dev/test)
│
├── dags/
│   └── dag_medallion_pipeline.py
│
├── tests/                         # Testes unitários (pytest)
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── notebooks/
│   └── gold_exploration.ipynb     # Análise exploratória das tabelas Gold
│
├── docs/
│   └── queries_gold.sql           # Consultas analíticas SQL prontas
│
├── docker/
│   └── init_gold_db.sql           # Criação do schema Gold no PostgreSQL
│
├── data/                          # Dados (ignorados pelo .gitignore)
│   ├── bronze/                    # Delta Lake Bronze
│   ├── silver/                    # Delta Lake Silver
│   └── gold/                      # Delta Lake Gold
│
├── docker-compose.yml             # Orquestração de containers
├── run_pipeline.py                # Runner standalone (sem Airflow)
├── Makefile                       # Atalhos de comandos
├── requirements.txt               # Dependências Python
├── pytest.ini                     # Configuração do pytest
├── conftest.py                    # Fixtures globais do pytest
└── .gitignore
```

---

##  Dataset

*Fonte:* [Portal da Transparência](https://www.transparencia.gov.br/download-de-dados/bolsa-familia-pagamentos) / [dados.gov.br](https://dados.gov.br)

*Dataset:* Pagamentos do Bolsa Família — Ministério do Desenvolvimento Social (MDS)

| Campo | Tipo Raw | Descrição |
|-------|----------|-----------|
| `MES_COMPETENCIA` | String | Período YYYYMM (ex: 202301) |
| `UF` | String | Unidade Federativa (2 letras) |
| `CODIGO_MUNICIPIO_SIAFI` | String | Código SIAFI do município |
| `NOME_MUNICIPIO` | String | Nome do município |
| `CPF_FAVORECIDO` | String | CPF mascarado pelo governo |
| `NIS_FAVORECIDO` | String | Número de Identificação Social (11 dígitos) |
| `NOME_FAVORECIDO` | String | Nome do beneficiário |
| `VALOR_PARCELA` | String | Valor pago (formato BR: "218,00") |

> *Modo de desenvolvimento:* por padrão, `USE_SAMPLE_DATA=true` gera 50.000 registros sintéticos realistas por mês, eliminando dependência de rede. Para usar dados reais, defina `USE_SAMPLE_DATA=false`.

---

##  Camadas Medallion

###  Bronze — Ingestão Raw

*Arquivo:* `src/bronze/bronze_ingestion.py`

A camada Bronze é o **ponto de entrada imutável** do dado. Preserva o CSV original sem qualquer transformação, adicionando apenas metadados de auditoria.

```
Entrada:  CSV do Portal da Transparência (separador ";", encoding Latin-1)
Saída:    Delta Lake particionado por ano/mes
Formato:  Todos os campos como String (sem inferência de tipos)
```

Características:
- *Idempotente:* não reprocessa se a partição já existir (a menos que `overwrite=True`)
- *Auditoria:* `_source`, `_ingested_at`, `_file_hash` (MD5)
- *Fallback:* gera dados sintéticos se a API estiver indisponível

###  Silver — Dados Limpos

*Arquivo:* `src/silver/silver_transform.py`

A camada Silver aplica **qualidade de dado**: tipagem correta, deduplicação e padronização.

```
Entrada:  Delta Lake Bronze
Saída:    Delta Lake Silver (particionado por ano/mes)
Campos:   snake_case, tipos corretos, sem duplicatas
```

| Transformação | Detalhe |
|--------------|---------|
| Rename | MES_COMPETENCIA → competencia, etc. |
| Tipagem | VALOR_PARCELA "218,00" → 218.0 (Double) |
| Extração | "202301" → ano=2023, mes=1 |
| Normalização | UPPER + TRIM em colunas de texto |
| Filtro UF | Remove UFs inválidas (XX, etc.) |
| Filtro Valor | Remove valor_parcela ≤ 0 |
| Drop Nulls | Campos críticos: nis, uf, valor, ano, mes |
| Deduplicação | Window por (nis + ano + mes), mantém maior valor |

###  Gold — Pronto para Análise

**Arquivo:** `src/gold/gold_aggregations.py`

A camada Gold entrega *tabelas analíticas* prontas para dashboards e relatórios.

#### Tabela 1: `transferencias_por_uf_mes`
```sql
SELECT ano, mes, uf,
       COUNT(nis)        AS total_beneficios,
       SUM(valor)        AS total_valor_brl,
       AVG(valor)        AS media_valor_brl
FROM silver GROUP BY ano, mes, uf
```

#### Tabela 2: `top_municipios_beneficios`
```sql
SELECT ano, municipio, uf,
       COUNT(nis)            AS total_beneficios,
       SUM(valor)            AS total_valor_brl,
       ROW_NUMBER() OVER ... AS ranking_nacional
FROM silver GROUP BY ano, municipio, uf
```

#### Tabela 3: `serie_temporal_brasil`
```sql
SELECT ano_mes,
       COUNT(nis)                      AS total_beneficios,
       SUM(valor)                      AS total_valor_brl,
       (count - LAG(count)) / LAG(count) * 100 AS variacao_pct
FROM silver GROUP BY ano_mes ORDER BY ano_mes
```

---

##  Como Rodar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) + [Docker Compose](https://docs.docker.com/compose/)
- Git

### Opção 1 — Com Docker (recomendado)

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/medallion-etl.git
cd medallion-etl

# 2. Suba o ambiente completo
make up
# ou: docker compose up -d

# 3. Aguarde ~2 minutos para inicialização
# Airflow: http://localhost:8080  (admin / admin)
# Jupyter: http://localhost:8888

# 4. Acesse o Airflow e ative a DAG:
#    → http://localhost:8080
#    → DAG: medallion_bolsa_familia
#    → Toggle ON → Trigger DAG

# 5. Ou dispare o pipeline via CLI:
docker compose exec airflow-scheduler \
  python /opt/airflow/run_pipeline.py --year 2023 --month 1
```

### Opção 2 — Local (sem Docker)

```bash
# 1. Python 3.11+ e Java 11+ são necessários para PySpark
java -version  # deve ser >= 11

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Configure as variáveis de ambiente
export MEDALLION_BASE_PATH=./data
export USE_SAMPLE_DATA=true    # usa dados sintéticos (sem internet)

# 4. Rode o pipeline completo
python run_pipeline.py --year 2023 --month 1

# 5. Ou use o Makefile
make pipeline YEAR=2023 MONTH=1
```

### Opção 3 — Meses múltiplos

```bash
# Processar todos os 12 meses de 2023
make pipeline-all YEAR=2023

# Ou via script
python run_pipeline.py --year 2023 --all-months

# Processar apenas Bronze e Silver
python run_pipeline.py --year 2023 --month 3 --layers bronze silver
```

### Verificando os dados gerados

```bash
# Estrutura de dados gerados
ls -la data/bronze/bolsa_familia/ano=2023/mes=01/
ls -la data/silver/bolsa_familia/ano=2023/mes=01/
ls -la data/gold/transferencias_por_uf_mes/

# No Jupyter (http://localhost:8888):
# Abra notebooks/gold_exploration.ipynb
```

### Configurações de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `MEDALLION_BASE_PATH` | `/opt/airflow/data` | Diretório base dos dados |
| `USE_SAMPLE_DATA` | `true` | Usa dados sintéticos (sem rede) |
| `PIPELINE_YEAR` | `2023` | Ano padrão do pipeline |
| `PIPELINE_MONTH` | `1` | Mês padrão do pipeline |
| `POSTGRES_HOST` | `postgres` | Host do PostgreSQL |
| `POSTGRES_DB` | `airflow` | Database PostgreSQL |

---

##  Testes

```bash
# Todos os testes
make test

# Com cobertura de código (relatório HTML)
make test-cov
# → abre docs/coverage/index.html

# Testes rápidos (sem PySpark)
make test-fast

# Teste específico
pytest tests/test_silver.py -v
pytest tests/test_gold.py::TestGoldAggregations::test_agg_por_uf_mes_sp_jan_total -v
```

### Cobertura dos testes

| Módulo | Testes |
|--------|--------|
| `sample_data.py` | Geração, formato, determinismo, UFs válidas |
| `config.py` | Paths, JDBC URL, colunas raw |
| `bronze_ingestion.py` | Mapeamento de colunas, variáveis de ambiente |
| `silver_transform.py` | Rename, tipagem, extração, normalização, filtros, dedup |
| `gold_aggregations.py` | Todas as 3 agregações, ranking, variação % |

---

##  Airflow DAG

*DAG ID:* `medallion_bolsa_familia`  
*Schedule:* `0 9 1 * *` (todo dia 1º do mês, 09:00 UTC)

```
[start]
   │
   ▼
[bronze_ingestion]      ← Download CSV + Delta Lake Bronze
   │
   ▼
[silver_transform]      ← Limpeza + tipagem + dedup
   │
   ▼
[gold_aggregations]     ← 3 tabelas analíticas
   │
   ▼
[data_quality_check]    ← Valida: bronze>0, silver≥70% do bronze, gold>0
   │
   ▼
[end]
```

### Trigger manual com parâmetros

No Airflow UI → **Trigger DAG w/ config**:
```json
{
  "year": 2023,
  "month": 6
}
```

### XCom entre tasks

| Task | Key | Valor |
|------|-----|-------|
| `bronze_ingestion` | `bronze_count` | Número de linhas ingeridas |
| `silver_transform` | `silver_count` | Número de linhas limpas |
| `gold_aggregations` | `gold_tables` | `{"tabela": n_linhas, ...}` |

### Data Quality Checks

O `data_quality_check` valida automaticamente:
- ✅ Bronze tem registros (`bronze_count > 0`)
- ✅ Silver reteve ≥ 70% dos dados do Bronze
- ✅ Todas as 3 tabelas Gold foram criadas com dados

---

##  Consultas SQL Gold

Execute no PostgreSQL (via `make up` + psql ou DBeaver):

```bash
# Conectar ao PostgreSQL
docker compose exec postgres psql -U airflow -d airflow
```

```sql
-- Top 5 estados por valor transferido
SELECT uf, SUM(total_valor_brl) AS total
FROM gold.transferencias_por_uf_mes
GROUP BY uf ORDER BY total DESC LIMIT 5;

-- Crescimento mês a mês
SELECT ano_mes, total_beneficios, variacao_pct
FROM gold.serie_temporal_brasil
ORDER BY ano_mes;

-- Top 10 municípios
SELECT ranking_nacional, municipio, uf, total_beneficios
FROM gold.top_municipios_beneficios
WHERE ranking_nacional <= 10;
```

Mais consultas em [`docs/queries_gold.sql`](docs/queries_gold.sql).

---

##  Stack Técnica

| Componente | Tecnologia | Versão | Papel |
|-----------|-----------|--------|-------|
| Processamento | Apache Spark (PySpark) | 3.5.0 | Engine ETL distribuída |
| Formato de Armazenamento | Delta Lake | 3.1.0 | ACID, time travel, schema evolution |
| Orquestração | Apache Airflow | 2.8.1 | Scheduling, monitoramento, retry |
| Banco de Dados | PostgreSQL | 15 | Camada Gold analítica (SQL) |
| Linguagem | Python | 3.11 | Implementação do pipeline |
| Containers | Docker + Compose | — | Ambiente reprodutível |
| Testes | pytest + pytest-cov | 7.4.4 | Qualidade do código |
| Notebook | Jupyter Lab | — | Exploração / validação |
| Fonte de Dados | dados.gov.br | — | Dataset público real |

### Por que Delta Lake?

- *ACID transactions:* garantia de consistência mesmo com falhas
- *Time travel:* `VERSION AS OF 5` para reprodutibilidade
- *Schema evolution:* colunas novas sem recriar a tabela
- *Upsert (MERGE):* atualização incremental eficiente
- **Partition pruning:* leitura apenas das partições necessárias

---

##  Licença

MIT License — veja [LICENSE](LICENSE) para detalhes.

---

##  Contribuindo

1. Fork o repositório
2. Crie uma branch: `git checkout -b feature/minha-feature`
3. Commit: `git commit -m 'feat: adiciona X'`
4. Push: `git push origin feature/minha-feature`
5. Abra um Pull Request

---

*Dados públicos fornecidos pelo Ministério do Desenvolvimento Social via [Portal da Transparência](https://www.transparencia.gov.br) e [dados.gov.br](https://dados.gov.br).*
