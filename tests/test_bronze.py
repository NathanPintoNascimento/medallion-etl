"""
test_bronze.py — Testes unitários da camada Bronze
"""

import pytest
from unittest.mock import patch, MagicMock


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def sample_csv_content():
    """CSV de amostra no formato do Portal da Transparência."""
    from src.utils.sample_data import generate_csv
    return generate_csv(year=2023, month=1, n_rows=100, seed=42)


# ── Testes do gerador de dados de amostra ─────────────────────────────────────

class TestSampleData:

    def test_generate_csv_returns_string(self, sample_csv_content):
        assert isinstance(sample_csv_content, str)

    def test_generate_csv_has_header(self, sample_csv_content):
        first_line = sample_csv_content.splitlines()[0]
        assert "MES_COMPETENCIA" in first_line
        assert "VALOR_PARCELA" in first_line

    def test_generate_csv_row_count(self, sample_csv_content):
        lines = [l for l in sample_csv_content.splitlines() if l.strip()]
        assert len(lines) == 101  # 1 header + 100 rows

    def test_generate_csv_correct_columns(self, sample_csv_content):
        header = sample_csv_content.splitlines()[0]
        cols   = header.split(";")
        assert len(cols) == 8

    def test_generate_csv_deterministic(self):
        from src.utils.sample_data import generate_csv
        csv1 = generate_csv(2023, 1, n_rows=10, seed=99)
        csv2 = generate_csv(2023, 1, n_rows=10, seed=99)
        assert csv1 == csv2

    def test_generate_csv_uf_values(self, sample_csv_content):
        """Verifica que apenas UFs válidas foram geradas."""
        lines = sample_csv_content.splitlines()[1:]
        ufs   = {line.split(";")[1] for line in lines if line}
        valid = {
            "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
            "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
            "SP","SE","TO",
        }
        assert ufs.issubset(valid), f"UFs inválidas encontradas: {ufs - valid}"

    def test_generate_csv_valor_format(self, sample_csv_content):
        """Valores devem estar no formato decimal brasileiro (vírgula)."""
        lines = sample_csv_content.splitlines()[1:5]
        for line in lines:
            valor = line.split(";")[-1].strip()
            assert "," in valor, f"Valor sem vírgula: {valor}"


# ── Testes de configuração ────────────────────────────────────────────────────

class TestConfig:

    def test_paths_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MEDALLION_BASE_PATH", str(tmp_path))
        # Re-import para pegar o novo env
        import importlib
        import src.utils.config as cfg
        importlib.reload(cfg)
        assert cfg.BRONZE_PATH.exists()
        assert cfg.SILVER_PATH.exists()
        assert cfg.GOLD_PATH.exists()

    def test_postgres_jdbc_url(self):
        from src.utils.config import PG_CONFIG
        assert PG_CONFIG.jdbc_url.startswith("jdbc:postgresql://")
        assert "airflow" in PG_CONFIG.jdbc_url

    def test_raw_columns_complete(self):
        from src.utils.config import RAW_COLUMNS
        expected = {
            "MES_COMPETENCIA", "UF", "CODIGO_MUNICIPIO_SIAFI",
            "NOME_MUNICIPIO", "CPF_FAVORECIDO", "NIS_FAVORECIDO",
            "NOME_FAVORECIDO", "VALOR_PARCELA",
        }
        assert set(RAW_COLUMNS) == expected


# ── Testes de ingestão (mock do Spark) ────────────────────────────────────────

class TestBronzeIngestion:

    def test_column_map_covers_raw_columns(self):
        """Todos os RAW_COLUMNS devem ter mapeamento Silver."""
        from src.utils.config import RAW_COLUMNS
        from src.silver.silver_transform import COLUMN_MAP
        for col in RAW_COLUMNS:
            assert col in COLUMN_MAP, f"Coluna sem mapeamento Silver: {col}"

    def test_use_sample_data_env(self, monkeypatch):
        monkeypatch.setenv("USE_SAMPLE_DATA", "true")
        import importlib
        import src.utils.config as cfg
        importlib.reload(cfg)
        assert cfg.USE_SAMPLE_DATA is True

    def test_logger_creates_instance(self):
        from src.utils.logger import get_logger
        log = get_logger("test_logger")
        assert log is not None
        assert log.name == "test_logger"
