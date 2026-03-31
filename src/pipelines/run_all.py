"""Ejecuta todos los pipelines del proyecto NAIRU."""

from __future__ import annotations

import logging

from src.pipelines import run_andi, run_banrep_inflation, run_brent, run_ipc, run_unemployment

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta desempleo + IPC + inflación BANREP + Brent + ANDI secuencialmente."""
    run_unemployment.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_brent.run()
    run_andi.run()
