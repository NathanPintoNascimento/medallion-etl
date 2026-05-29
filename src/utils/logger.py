"""
logger.py — Configuração de logging para o pipeline
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


def get_logger(name: str, log_dir: str | None = None) -> logging.Logger:
    """
    Retorna logger com handlers para console e (opcionalmente) arquivo.

    Args:
        name:    Nome do módulo/logger.
        log_dir: Diretório para gravar arquivo de log. None = só console.
    """
    logger = logging.getLogger(name)

    if logger.handlers:          # evita duplicação em chamadas repetidas
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Arquivo (opcional)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        ts  = datetime.now().strftime("%Y%m%d")
        fh  = logging.FileHandler(f"{log_dir}/{name}_{ts}.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
