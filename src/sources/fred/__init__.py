"""Fuentes de datos de FRED (Federal Reserve Economic Data)."""

from src.sources.fred.brent import run_brent_pipeline

__all__ = [
    "run_brent_pipeline",
]
