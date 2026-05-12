"""Fuentes de datos del DANE: desempleo (GEIH), IPC y PIB.

Re-exporta las funciones-pipeline de cada submódulo. Uso recomendado::

    from src.sources.dane import (
        run_geih_pipeline,
        run_ipc_pipeline,
        run_dane_gdp_pipeline,
    )
"""

from src.sources.dane.gdp import run_dane_gdp_pipeline
from src.sources.dane.ipc import (
    clean_ipc_data,
    run_ipc_pipeline,
    save_ipc_data,
)
from src.sources.dane.unemployment import (
    run_geih_pipeline,
    save_processed_data,
)

__all__ = [
    "clean_ipc_data",
    "run_dane_gdp_pipeline",
    "run_geih_pipeline",
    "run_ipc_pipeline",
    "save_ipc_data",
    "save_processed_data",
]
