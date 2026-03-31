"""Fuentes de datos del Banco de la República (SUAMECA)."""

from src.sources.banrep.inflation import run_banrep_inflation_pipeline

__all__ = [
    "run_banrep_inflation_pipeline",
]
