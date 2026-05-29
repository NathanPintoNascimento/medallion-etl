"""
test_silver.py — Testes unitários das transformações Silver
Usa PySpark local (sem Delta Lake) para testar as funções de transformação.
"""

import pytest

# ── Fixture: SparkSession local leve ─────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    """SparkSession local para testes — sem Delta Lake."""
    try:
        from pyspark.sql import SparkSession
        s = (
            SparkSession.builder
            .appName("TestSilver")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.memory", "512m")
            .getOrCreate()
        )
        s.sparkContext.setLogLevel("ERROR")
        yield s
        s.stop()
    except ImportError:
        pytest.skip("PySpark não disponível — pulando testes Spark")


@pytest.fixture()
def df_raw(spark):
    """DataFrame mock no formato Bronze."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("MES_COMPETENCIA",        StringType()),
        StructField("UF",                     StringType()),
        StructField("CODIGO_MUNICIPIO_SIAFI", StringType()),
        StructField("NOME_MUNICIPIO",         StringType()),
        StructField("CPF_FAVORECIDO",         StringType()),
        StructField("NIS_FAVORECIDO",         StringType()),
        StructField("NOME_FAVORECIDO",        StringType()),
        StructField("VALOR_PARCELA",          StringType()),
    ])
    data = [
        ("202301", "SP", "3550308", "sao paulo",   "***.123.456-**", "12345678901", "maria silva",   "218,00"),
        ("202301", "RJ", "3304557", "RIO DE JANEIRO", "***.789.012-**", "98765432100", "JOSE SANTOS",   "300,50"),
        ("202301", "MG", "3106200", "BELO HORIZONTE","***.345.678-**", "11122233344", "ANA OLIVEIRA",  "400,75"),
        ("202301", "XX", "9999999", "INVALIDA",     "***.000.000-**", "00000000000", "TESTE INVALIDO", "-50,00"),  # UF inválida
        ("202301", "BA", "2927408", "SALVADOR",     "***.111.222-**", None,           "SEM NIS",        "218,00"),  # NIS nulo
        # Duplicata do primeiro registro
        ("202301", "SP", "3550308", "SAO PAULO",    "***.123.456-**", "12345678901", "MARIA SILVA",   "250,00"),
    ]
    return spark.createDataFrame(data, schema)


# ── Testes das funções individuais ────────────────────────────────────────────

class TestSilverTransformFunctions:

    def test_rename_columns(self, spark, df_raw):
        from src.silver.silver_transform import _rename_columns
        df = _rename_columns(df_raw)
        assert "competencia"      in df.columns
        assert "uf"               in df.columns
        assert "valor_parcela_raw" in df.columns
        assert "MES_COMPETENCIA"  not in df.columns

    def test_parse_valor_virgula(self, spark, df_raw):
        from src.silver.silver_transform import _rename_columns, _parse_valor
        df = _parse_valor(_rename_columns(df_raw))
        assert "valor_parcela" in df.columns
        row = df.filter(df["nis_favorecido"] == "12345678901").first()
        assert abs(row["valor_parcela"] - 218.0) < 0.01

    def test_extract_periodo(self, spark, df_raw):
        from src.silver.silver_transform import _rename_columns, _parse_valor, _extract_periodo
        df = _extract_periodo(_parse_valor(_rename_columns(df_raw)))
        row = df.first()
        assert row["ano"] == 2023
        assert row["mes"] == 1

    def test_normalize_strings_uppercase(self, spark, df_raw):
        from src.silver.silver_transform import (
            _rename_columns, _parse_valor, _extract_periodo, _normalize_strings
        )
        df = _normalize_strings(_extract_periodo(_parse_valor(_rename_columns(df_raw))))
        # "sao paulo" deve virar "SAO PAULO"
        row = df.filter(df["nis_favorecido"] == "12345678901").first()
        assert row["nome_municipio"] == "SAO PAULO"

    def test_filter_invalid_values_removes_negative(self, spark, df_raw):
        from src.silver.silver_transform import (
            _rename_columns, _parse_valor, _extract_periodo,
            _normalize_strings, _filter_invalid_values,
        )
        df = _filter_invalid_values(
            _normalize_strings(_extract_periodo(_parse_valor(_rename_columns(df_raw))))
        )
        # Registro com valor -50 e UF XX deve ser removido
        invalid = df.filter(df["nis_favorecido"] == "00000000000").count()
        assert invalid == 0

    def test_filter_nulls_removes_null_nis(self, spark, df_raw):
        from src.silver.silver_transform import (
            _rename_columns, _parse_valor, _extract_periodo,
            _normalize_strings, _filter_invalid_values, _filter_nulls,
        )
        df = _filter_invalid_values(
            _normalize_strings(_extract_periodo(_parse_valor(_rename_columns(df_raw))))
        )
        df_clean, dropped = _filter_nulls(df)
        # Registro com NIS nulo deve ser removido
        assert dropped >= 1

    def test_deduplicate_keeps_highest_value(self, spark, df_raw):
        """Deduplicação deve manter o registro com maior valor_parcela."""
        from src.silver.silver_transform import (
            _rename_columns, _parse_valor, _extract_periodo,
            _normalize_strings, _filter_invalid_values, _filter_nulls, _deduplicate,
        )
        df = _filter_nulls(
            _filter_invalid_values(
                _normalize_strings(_extract_periodo(_parse_valor(_rename_columns(df_raw))))
            )
        )[0]
        df_dedup, removed = _deduplicate(df)

        # NIS 12345678901 aparecia 2x — deve ficar 1x com valor maior (250,00)
        rows = df_dedup.filter(df_dedup["nis_favorecido"] == "12345678901").collect()
        assert len(rows) == 1
        assert abs(rows[0]["valor_parcela"] - 250.0) < 0.01
        assert removed >= 1

    def test_full_pipeline_row_count(self, spark, df_raw):
        """Pipeline completo deve retornar 3 linhas válidas."""
        from src.silver.silver_transform import (
            _rename_columns, _parse_valor, _extract_periodo,
            _normalize_strings, _filter_invalid_values, _filter_nulls, _deduplicate,
        )
        df = df_raw
        df = _rename_columns(df)
        df = _parse_valor(df)
        df = _extract_periodo(df)
        df = _normalize_strings(df)
        df = _filter_invalid_values(df)
        df, _ = _filter_nulls(df)
        df, _ = _deduplicate(df)
        # 6 raw → remove XX+negativo (1) + NIS nulo (1) + duplicata (1) = 3 restantes
        assert df.count() == 3


# ── Testes de schema ─────────────────────────────────────────────────────────

class TestSilverSchema:

    def test_silver_schema_has_required_cols(self):
        from src.utils.config import SILVER_SCHEMA
        required = {"ano", "mes", "uf", "nome_municipio", "nis_favorecido",
                    "valor_parcela", "dt_ingestao"}
        assert required.issubset(set(SILVER_SCHEMA.keys()))

    def test_valor_parcela_is_double(self):
        from src.utils.config import SILVER_SCHEMA
        assert SILVER_SCHEMA["valor_parcela"] == "double"

    def test_ano_mes_are_integer(self):
        from src.utils.config import SILVER_SCHEMA
        assert SILVER_SCHEMA["ano"] == "integer"
        assert SILVER_SCHEMA["mes"] == "integer"
