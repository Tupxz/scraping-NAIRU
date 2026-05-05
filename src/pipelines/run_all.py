"""Ejecuta todos los pipelines del proyecto NAIRU."""

from __future__ import annotations

import logging

from src.pipelines import (
    run_andi,
    run_banrep_inflation,
    run_banrep_tes,
    run_brent,
    run_dane_gdp,
    run_informality,
    run_ipc,
    run_merge,
    run_pwt,
    run_unemployment,
    run_viog,
)

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta todos los pipelines + merge secuencialmente."""
    run_unemployment.run()
    run_informality.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_banrep_tes.run()
    run_brent.run()
    run_andi.run()
    run_pwt.run()
    run_dane_gdp.run()
    run_viog.run()
    run_viog.run_colombia()  # skip elegante si data/inputs/PIB_CO.xlsx no existe
    run_merge.run()
