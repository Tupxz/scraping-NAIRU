"""Ejecuta todos los pipelines del proyecto NAIRU.

Equivalente programático a ``python -m src.main --all``: corre cada
pipeline secuencialmente (network-bound; no se paraleliza por defecto
para mantener orden determinístico de logs y evitar saturar las APIs
del DANE/BANREP) y termina con el merge consolidado.

El orden importa: VIOG-Colombia depende de ``dane_gdp`` (lee
``dane_gdp_colombia.csv``), y el merge depende de todos los pipelines
de fuentes; por eso se ejecutan al final.
"""

from __future__ import annotations

import logging

from src.pipelines import (
    run_andi,
    run_banrep_inflation,
    run_banrep_tes,
    run_brent,
    run_dane_gdp,
    run_ipc,
    run_merge,
    run_pwt,
    run_unemployment,
    run_viog,
)

logger = logging.getLogger("nairu_pipeline")


def run() -> None:
    """Ejecuta todos los pipelines + merge secuencialmente."""
    # Fuentes mensuales
    run_unemployment.run()
    run_ipc.run()
    run_banrep_inflation.run()
    run_banrep_tes.run()
    run_brent.run()
    run_andi.run()

    # Fuentes anuales / trimestrales
    run_pwt.run()
    run_dane_gdp.run()

    # VIOG (depende de dane_gdp para Colombia)
    run_viog.run()
    run_viog.run_colombia()  # skip elegante si data/inputs/PIB_CO.xlsx no existe

    # Consolidación final
    run_merge.run()
