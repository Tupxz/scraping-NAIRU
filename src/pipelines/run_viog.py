"""Pipeline runner para el VIOG (output gap ponderado por filtros).

Soporta dos países:
  - **USA** (default histórico): VIOG_CONFIG, lee PIB_USA.xlsx.
  - **Colombia**: VIOG_CO_CONFIG, lee PIB_CO.xlsx (input manual con
    serie observada del DANE + PIB potencial estimado externamente).

El runner para Colombia hace **skip elegante** si el archivo de input
no existe (escenario habitual hasta que se reciba el PIB potencial),
para no romper ``--all``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.config import (
    INPUTS_DIR,
    OUTPUTS_DIR,
    PROCESSED_DIR,
    VIOG_CO_CONFIG,
    VIOG_CONFIG,
    VIOGConfig,
)
from src.sources.viog.viog import run_viog_pipeline

logger = logging.getLogger("nairu_pipeline.pipelines.viog")


def _run_for_config(
    config: VIOGConfig,
    *,
    plot_subdir: str,
    skip_if_missing: bool = False,
) -> None:
    """Corre el pipeline VIOG con una configuración específica.

    Parameters
    ----------
    config : VIOGConfig
        Configuración del país (VIOG_CONFIG / VIOG_CO_CONFIG).
    plot_subdir : str
        Subcarpeta de outputs/ donde se guardan las gráficas.
    skip_if_missing : bool
        Si True y el archivo de input no existe, emite warning y
        retorna sin error. Útil para Colombia hasta que llegue el
        PIB potencial.
    """
    input_path = INPUTS_DIR / config.input_filename
    output_path = PROCESSED_DIR / config.processed_filename
    plot_dir = OUTPUTS_DIR / plot_subdir

    if not input_path.exists():
        msg = f"[VIOG] Input no encontrado: {input_path}"
        if skip_if_missing:
            logger.warning(msg + " — pipeline omitido.")
            print(msg + " — pipeline omitido.")
            return
        raise FileNotFoundError(msg)

    df = run_viog_pipeline(input_path, output_path, plot=True, plot_dir=plot_dir)
    print(f"[VIOG] {len(df)} observaciones guardadas en {output_path}")
    print(f"[VIOG] Gráficas guardadas en {plot_dir}")


def run() -> None:
    """Ejecuta VIOG para USA (comportamiento histórico/default)."""
    _run_for_config(VIOG_CONFIG, plot_subdir="viog")


def run_colombia() -> None:
    """Ejecuta VIOG para Colombia.

    Si ``data/inputs/PIB_CO.xlsx`` no existe (ej. aún no se recibió
    el PIB potencial estimado por función de producción), emite un
    warning y omite el pipeline sin fallar.
    """
    _run_for_config(VIOG_CO_CONFIG, plot_subdir="viog_colombia", skip_if_missing=True)
