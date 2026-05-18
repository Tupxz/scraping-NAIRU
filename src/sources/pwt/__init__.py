"""Fuente de datos Penn World Tables 11.0 (stock de capital + capital humano).

Re-exporta las funciones-pipeline para mantener simetría con
``src/sources/dane``, ``src/sources/banrep``, etc.
"""

from src.sources.pwt.pwt import (
    download_pwt_csv,
    parse_pwt_csv,
    run_pwt_pipeline,
)

__all__ = [
    "download_pwt_csv",
    "parse_pwt_csv",
    "run_pwt_pipeline",
]
