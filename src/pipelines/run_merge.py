"""Pipeline de merge: une todas las bases procesadas en un dataset único.

Orquesta: carga CSVs → outer-merge por fecha → guardado nairu_dataset.csv.
"""

from __future__ import annotations

import logging

from src.io_utils import setup_logging
from src.merge import run_merge_pipeline
from src.quality_checks import run_derived_checks

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta el pipeline de merge."""
    setup_logging()
    logger.info("── Pipeline MERGE (dataset unificado) ──")

    df = run_merge_pipeline()
    run_derived_checks(df)

    logger.info(
        "Dataset unificado: %d filas × %d columnas",
        len(df), len(df.columns),
    )
