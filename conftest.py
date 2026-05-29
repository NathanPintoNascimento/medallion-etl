# conftest.py — Configuração global do pytest

import sys
import os
from pathlib import Path

# Garante que src/ está no PYTHONPATH para imports
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# Usa dados de amostra em todos os testes (sem chamadas de rede)
os.environ.setdefault("USE_SAMPLE_DATA", "true")
os.environ.setdefault("MEDALLION_BASE_PATH", str(ROOT / "data" / "test"))
