"""
sample_data.py — Gera dados de exemplo realistas para dev/test
Simula o formato real dos CSVs do Portal da Transparência (Bolsa Família)
"""

from __future__ import annotations
import csv
import io
import random
from datetime import date

# ── Dados geográficos reais (amostra) ──────────────────────────────────────
MUNICIPIOS: list[tuple[str, str, str]] = [
    ("SP", "3550308", "SAO PAULO"),
    ("SP", "3509502", "CAMPINAS"),
    ("SP", "3543402", "RIBEIRAO PRETO"),
    ("RJ", "3304557", "RIO DE JANEIRO"),
    ("RJ", "3301009", "CAMPOS DOS GOYTACAZES"),
    ("MG", "3106200", "BELO HORIZONTE"),
    ("MG", "3170206", "UBERLANDIA"),
    ("BA", "2927408", "SALVADOR"),
    ("BA", "2910800", "FEIRA DE SANTANA"),
    ("CE", "2304400", "FORTALEZA"),
    ("CE", "2307650", "JUAZEIRO DO NORTE"),
    ("PE", "2611606", "RECIFE"),
    ("PE", "2604106", "CARUARU"),
    ("AM", "1302603", "MANAUS"),
    ("PA", "1501402", "BELEM"),
    ("GO", "5208707", "GOIANIA"),
    ("RS", "4314902", "PORTO ALEGRE"),
    ("SC", "4205407", "FLORIANOPOLIS"),
    ("PR", "4106902", "CURITIBA"),
    ("MA", "2111300", "SAO LUIS"),
    ("PI", "2211001", "TERESINA"),
    ("RN", "2408102", "NATAL"),
    ("AL", "2704302", "MACEIO"),
    ("SE", "2800308", "ARACAJU"),
    ("MT", "5103403", "CUIABA"),
    ("MS", "5002704", "CAMPO GRANDE"),
    ("RO", "1100205", "PORTO VELHO"),
    ("TO", "1721000", "PALMAS"),
    ("AC", "1200401", "RIO BRANCO"),
    ("AP", "1600303", "MACAPA"),
    ("RR", "1400100", "BOA VISTA"),
]

NOMES_PRIMEIRO = [
    "MARIA", "JOSE", "ANA", "JOAO", "FRANCISCA", "ANTONIO", "ADRIANA",
    "PAULO", "FATIMA", "LUIZ", "SANDRA", "CARLOS", "PAULA", "MARCOS",
    "LUCIANA", "PEDRO", "PATRICIA", "ROBERTO", "JULIANA", "GABRIEL",
]

NOMES_SOBRENOME = [
    "SILVA", "SANTOS", "OLIVEIRA", "SOUZA", "RODRIGUES", "FERREIRA",
    "ALVES", "PEREIRA", "LIMA", "GOMES", "COSTA", "RIBEIRO", "MARTINS",
    "CARVALHO", "ALMEIDA", "LOPES", "SOARES", "FERNANDES", "VIEIRA",
]


def _random_cpf() -> str:
    """CPF mascarado — formato real do Portal da Transparência."""
    d = [random.randint(0, 9) for _ in range(11)]
    return f"***.{d[3]}{d[4]}{d[5]}.{d[6]}{d[7]}{d[8]}-**"


def _random_nis() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(11))


def _random_nome() -> str:
    return f"{random.choice(NOMES_PRIMEIRO)} {random.choice(NOMES_SOBRENOME)}"


def _random_valor() -> str:
    """Valor parcela — distribuição realista (R$ 200 – R$ 800)."""
    base   = random.choices([218.0, 300.0, 400.0, 600.0, 800.0],
                            weights=[40, 25, 20, 10, 5])[0]
    ruido  = random.uniform(-20, 20)
    return f"{base + ruido:.2f}".replace(".", ",")


def generate_csv(
    year: int,
    month: int,
    n_rows: int = 5_000,
    seed: int | None = None,
) -> str:
    """
    Gera CSV no formato do Portal da Transparência (Bolsa Família).

    Returns:
        String CSV com separador ';' e encoding Latin-1 simulado (str UTF-8).
    """
    if seed is not None:
        random.seed(seed)

    competencia = f"{year}{month:02d}"   # ex: 202301

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)

    # Cabeçalho exato do Portal da Transparência
    writer.writerow([
        "MES_COMPETENCIA",
        "UF",
        "CODIGO_MUNICIPIO_SIAFI",
        "NOME_MUNICIPIO",
        "CPF_FAVORECIDO",
        "NIS_FAVORECIDO",
        "NOME_FAVORECIDO",
        "VALOR_PARCELA",
    ])

    for _ in range(n_rows):
        uf, cod, mun = random.choice(MUNICIPIOS)
        writer.writerow([
            competencia,
            uf,
            cod,
            mun,
            _random_cpf(),
            _random_nis(),
            _random_nome(),
            _random_valor(),
        ])

    return buf.getvalue()


if __name__ == "__main__":
    print(generate_csv(2023, 1, n_rows=10, seed=42))
