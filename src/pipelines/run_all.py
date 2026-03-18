"""Ejecuta todos los pipelines del proyecto NAIRU."""

from __future__ import annotations

import logging

from src.pipelines import run_ipc, run_unemployment

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta desempleo + IPC secuencialmente."""
    run_unemployment.run()
    run_ipc.run()
