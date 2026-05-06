"""Pipeline NAIRU/NAICU estimation v6.

Ejecuta la estimación usando ``Data_NAIRU.xlsx`` construido por
``build_nairu_dataset.py``.  Los outputs se guardan en ``outputs/nairu/``.

Orden recomendado::

    python -m src.main --nairu-dataset  # construye Data_NAIRU.xlsx
    python -m src.main --nairu-estim    # estima NAIRU/NAICU v6

o simplemente ``python -m src.main --all``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("nairu_pipeline.nairu_estimation")


def run() -> None:
    """Ejecuta la estimacion NAIRU v6 desde el pipeline."""
    from src.nairu.estimation import build_outputs
    summary = build_outputs()
    first_lines = "\n".join(summary.splitlines()[:12])
    logger.info("[NAIRU-v6] Resumen:\n%s", first_lines)
