"""Orquestador NAIRU/NAICU estimation v6.

Refactor de ``nairu_estimation_v6.py`` (raíz del proyecto) como módulo
propio dentro de ``src/nairu/``.  Todos los paths son absolutos y derivan
de la configuración del proyecto (``src/config.py``).

Uso programático
----------------
    from src.nairu.estimation import build_outputs
    summary = build_outputs()

Modelo core
-----------
El modelo estadístico vive en ``src/nairu/model_core.py`` y debe exportar::

    load_and_prepare_data(data_path: Path) -> pd.DataFrame
    build_model_data(data: pd.DataFrame) -> dict
    estimate_parameters(model_data: dict) -> object
    build_outputs(data, model_data, fit, output_dir: Path) -> tuple[Path, str]

Si ``model_core.py`` no existe, el pipeline falla con un mensaje claro
indicando dónde colocarlo.
"""

from __future__ import annotations

import importlib.util
import logging
import shutil
import sys
from pathlib import Path
from types import ModuleType

from src.config import INPUTS_DIR, OUTPUTS_DIR

logger = logging.getLogger("nairu_pipeline.nairu_estimation")

# ── Paths canónicos del módulo ────────────────────────────────────────
DATA_NAIRU_PATH: Path = INPUTS_DIR / "Data_NAIRU.xlsx"
OUTPUT_DIR:      Path = OUTPUTS_DIR / "nairu"
MODEL_CORE_PATH: Path = Path(__file__).resolve().parent / "model_core.py"

# Nombres de archivo: desde el modelo (v5) → repo (v6)
_V5_TO_V6: dict[str, str] = {
    "nairu_estimates_v5.csv":      "nairu_estimates_v6.csv",
    "nairu_summary_v5.txt":        "nairu_summary_v6.txt",
    "nairu_mle_coefficients_v5.csv": "nairu_mle_coefficients_v6.csv",
    "nairu_mle_covariance_v5.csv": "nairu_mle_covariance_v6.csv",
    "nairu_mle_diagnostics_v5.txt": "nairu_mle_diagnostics_v6.txt",
    "nairu_naicu_panel_v5.png":    "nairu_naicu_panel_v6.png",
    "nairu_naicu_panel_v5.svg":    "nairu_naicu_panel_v6.svg",
}


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _load_model_core() -> ModuleType:
    """Carga ``src/nairu/model_core.py`` como módulo dinámico."""
    if not MODEL_CORE_PATH.exists():
        raise FileNotFoundError(
            f"Modelo estadístico no encontrado: {MODEL_CORE_PATH}\n"
            "Coloca el código de 'nairu_estimation_v5.py' en ese archivo.\n"
            "El módulo debe exponer: load_and_prepare_data, build_model_data, "
            "estimate_parameters, build_outputs."
        )
    spec = importlib.util.spec_from_file_location("nairu_model_core", MODEL_CORE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar {MODEL_CORE_PATH}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _rewrite_v5_to_v6(text: str) -> str:
    return (
        text
        .replace("Estimation v5", "Estimation v6")
        .replace("nairu_naicu_panel_v5.png", "nairu_naicu_panel_v6.png")
        .replace("nairu_naicu_panel_v5.svg", "nairu_naicu_panel_v6.svg")
    )


def _copy_outputs(src_dir: Path, dst_dir: Path) -> None:
    """Copia y renombra outputs v5 → v6."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    for v5_name, v6_name in _V5_TO_V6.items():
        src_file = src_dir / v5_name
        dst_file = dst_dir / v6_name
        if not src_file.exists():
            logger.warning("[NAIRU-v6] Archivo no encontrado para copiar: %s", src_file)
            continue
        if v5_name.endswith(".txt"):
            text = src_file.read_text(encoding="utf-8")
            dst_file.write_text(_rewrite_v5_to_v6(text), encoding="utf-8")
        else:
            shutil.copy2(src_file, dst_file)
    logger.info("[NAIRU-v6] Outputs copiados a %s", dst_dir)


# ═══════════════════════════════════════════════════════════════════════
# API pública
# ═══════════════════════════════════════════════════════════════════════

def build_outputs(
    data_path: Path | None = None,
    output_dir: Path | None = None,
) -> str:
    """Ejecuta la estimación NAIRU/NAICU v6 y escribe los outputs.

    Estrategia
    ----------
    1. Si ``output_dir`` ya contiene resultados previos válidos (``nairu_estimates_v6.csv``),
       los reporta sin re-estimar.
    2. En caso contrario, carga ``model_core.py``, lo ejecuta con
       ``data_path`` (``data/inputs/Data_NAIRU.xlsx``) y guarda los outputs.

    Parameters
    ----------
    data_path:
        Path al Excel de entrada.  Default: ``data/inputs/Data_NAIRU.xlsx``.
    output_dir:
        Directorio de salida.  Default: ``outputs/nairu/``.

    Returns
    -------
    str
        Texto del summary de la estimación.
    """
    data_path  = data_path  or DATA_NAIRU_PATH
    output_dir = output_dir or OUTPUT_DIR

    if not data_path.exists():
        raise FileNotFoundError(
            f"Data_NAIRU.xlsx no encontrado en {data_path}. "
            "Ejecuta primero: python -m src.main --nairu-dataset"
        )

    tmp_dir = output_dir / "_v6_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    # ── Ruta 0: resultados pre-existentes en data/inputs/ (seed inicial) ──
    # Si outputs/nairu/ aún no tiene resultados pero data/inputs/ tiene el
    # CSV de una ejecución previa, los copiamos como punto de partida.
    seed_csv = INPUTS_DIR / "nairu_estimates_v6.csv"
    existing_csv = output_dir / "nairu_estimates_v6.csv"
    if not existing_csv.exists() and seed_csv.exists():
        logger.info(
            "[NAIRU-v6] Copiando resultados pre-existentes desde %s → %s",
            seed_csv, output_dir,
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(seed_csv, existing_csv)

    # ── Ruta 1: outputs ya existían de una ejecución anterior ─────────
    if existing_csv.exists():
        summary_path = output_dir / "nairu_summary_v6.txt"
        summary = (
            summary_path.read_text(encoding="utf-8")
            if summary_path.exists() else
            f"[NAIRU-v6] Resultados cargados desde {output_dir} (no re-estimado)."
        )
        logger.info(
            "[NAIRU-v6] Resultados en %s — se omite re-estimación "
            "(para forzar: borra outputs/nairu/ y corre de nuevo).",
            output_dir,
        )
        return summary

    # ── Ruta 2: correr el modelo ──────────────────────────────────────
    logger.info("[NAIRU-v6] Cargando modelo core desde %s …", MODEL_CORE_PATH)
    module = _load_model_core()

    logger.info("[NAIRU-v6] Cargando y preparando datos desde %s …", data_path)
    data = module.load_and_prepare_data(data_path)

    logger.info("[NAIRU-v6] Construyendo dataset del modelo …")
    model_data = module.build_model_data(data)

    logger.info("[NAIRU-v6] Estimando parámetros (MLE) …")
    fit = module.estimate_parameters(model_data)

    logger.info("[NAIRU-v6] Generando outputs en %s …", tmp_dir)
    try:
        _, summary_v5 = module.build_outputs(data, model_data, fit, tmp_dir)
        summary = _rewrite_v5_to_v6(summary_v5)

        # Copiar y renombrar v5 → v6
        _copy_outputs(tmp_dir, output_dir)
        summary_path = output_dir / "nairu_summary_v6.txt"
        summary_path.write_text(summary, encoding="utf-8")

        logger.info("[NAIRU-v6] Estimación completada. Outputs en %s", output_dir)
        return summary
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)


def run() -> str:
    """Entry-point para el pipeline principal."""
    return build_outputs()
