"""
test_gold.py — Testes unitários das agregações Gold
"""

import pytest


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        s = (
            SparkSession.builder
            .appName("TestGold")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.memory", "512m")
            .getOrCreate()
        )
        s.sparkContext.setLogLevel("ERROR")
        yield s
        s.stop()
    except ImportError:
        pytest.skip("PySpark não disponível")


@pytest.fixture()
def df_silver(spark):
    """DataFrame Silver mock para testar agregações."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType
    )
    schema = StructType([
        StructField("ano",              IntegerType()),
        StructField("mes",              IntegerType()),
        StructField("uf",               StringType()),
        StructField("codigo_municipio", StringType()),
        StructField("nome_municipio",   StringType()),
        StructField("nis_favorecido",   StringType()),
        StructField("nome_favorecido",  StringType()),
        StructField("valor_parcela",    DoubleType()),
    ])
    data = [
        (2023, 1, "SP", "3550308", "SAO PAULO",      "11111111111", "ALICE",   300.0),
        (2023, 1, "SP", "3550308", "SAO PAULO",      "22222222222", "BOB",     200.0),
        (2023, 1, "SP", "3509502", "CAMPINAS",       "33333333333", "CARLOS",  250.0),
        (2023, 1, "RJ", "3304557", "RIO DE JANEIRO", "44444444444", "DIANA",   400.0),
        (2023, 1, "RJ", "3304557", "RIO DE JANEIRO", "55555555555", "EVA",     350.0),
        (2023, 2, "SP", "3550308", "SAO PAULO",      "66666666666", "FRANK",   500.0),
        (2023, 2, "MG", "3106200", "BELO HORIZONTE", "77777777777", "GRACE",   218.0),
    ]
    return spark.createDataFrame(data, schema)


class TestGoldAggregations:

    def test_agg_por_uf_mes_row_count(self, spark, df_silver):
        from src.gold.gold_aggregations import _agg_por_uf_mes
        df = _agg_por_uf_mes(df_silver)
        # SP jan, RJ jan, SP fev, MG fev = 4 combinações
        assert df.count() == 4

    def test_agg_por_uf_mes_sp_jan_total(self, spark, df_silver):
        from pyspark.sql import functions as F
        from src.gold.gold_aggregations import _agg_por_uf_mes
        df = _agg_por_uf_mes(df_silver)
        row = df.filter((F.col("uf") == "SP") & (F.col("mes") == 1)).first()
        assert row["total_beneficios"] == 3
        assert abs(row["total_valor_brl"] - 750.0) < 0.01
        assert abs(row["media_valor_brl"] - 250.0) < 0.01

    def test_agg_top_municipios_has_ranking(self, spark, df_silver):
        from src.gold.gold_aggregations import _agg_top_municipios
        df = _agg_top_municipios(df_silver)
        assert "ranking_nacional" in df.columns
        # Ranking 1 deve ter o maior número de benefícios
        top1 = df.filter(df["ranking_nacional"] == 1).first()
        assert top1["total_beneficios"] >= df.filter(df["ranking_nacional"] == 2).first()["total_beneficios"]

    def test_agg_top_municipios_sp_has_most(self, spark, df_silver):
        from pyspark.sql import functions as F
        from src.gold.gold_aggregations import _agg_top_municipios
        df = _agg_top_municipios(df_silver)
        # SP/SAO PAULO tem 3 registros no total (jan 2 + fev 1)
        sp = df.filter((F.col("nome_municipio") == "SAO PAULO") & (F.col("uf") == "SP")).first()
        assert sp["total_beneficios"] == 3

    def test_agg_serie_temporal_row_count(self, spark, df_silver):
        from src.gold.gold_aggregations import _agg_serie_temporal
        df = _agg_serie_temporal(df_silver)
        # 2 meses distintos: 2023-01, 2023-02
        assert df.count() == 2

    def test_agg_serie_temporal_variacao_first_is_null(self, spark, df_silver):
        """Primeiro mês da série não deve ter variação (sem mês anterior)."""
        from pyspark.sql import functions as F
        from src.gold.gold_aggregations import _agg_serie_temporal
        df = _agg_serie_temporal(df_silver)
        first = df.orderBy("ano_mes").first()
        assert first["variacao_pct"] is None

    def test_agg_serie_temporal_variacao_second(self, spark, df_silver):
        """Segundo mês deve ter variação calculada."""
        from pyspark.sql import functions as F
        from src.gold.gold_aggregations import _agg_serie_temporal
        df = _agg_serie_temporal(df_silver)
        second = df.orderBy("ano_mes").collect()[1]
        assert second["variacao_pct"] is not None

    def test_agg_serie_temporal_anomes_format(self, spark, df_silver):
        """Coluna ano_mes deve ter formato YYYY-MM."""
        from src.gold.gold_aggregations import _agg_serie_temporal
        df = _agg_serie_temporal(df_silver)
        rows = [r["ano_mes"] for r in df.collect()]
        for ym in rows:
            assert len(ym) == 7, f"Formato inválido: {ym}"
            assert ym[4] == "-", f"Separador ausente: {ym}"

    def test_gold_tables_all_present(self, spark, df_silver):
        """Verifica que as 3 funções de agregação retornam DataFrames não-vazios."""
        from src.gold.gold_aggregations import (
            _agg_por_uf_mes, _agg_top_municipios, _agg_serie_temporal
        )
        assert _agg_por_uf_mes(df_silver).count()    > 0
        assert _agg_top_municipios(df_silver).count() > 0
        assert _agg_serie_temporal(df_silver).count() > 0
