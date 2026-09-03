"""Orquestador NAIRU/NAICU estimation.

Módulo propio dentro de ``src/nairu/``.  Todos los paths son absolutos y derivan
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

# Nombres de archivo: desde el modelo (v5) → outputs del repo
_V5_TO_FINAL: dict[str, str] = {
    "nairu_estimates_v5.csv":        "nairu_colombia.csv",
    "nairu_summary_v5.txt":          "nairu_summary.txt",
    "nairu_mle_coefficients_v5.csv": "nairu_mle_coefficients.csv",
    "nairu_mle_covariance_v5.csv":   "nairu_mle_covariance.csv",
    "nairu_mle_diagnostics_v5.txt":  "nairu_mle_diagnostics.txt",
    "nairu_naicu_panel_v5.png":      "nairu_naicu_panel.png",
    "nairu_naicu_panel_v5.svg":      "nairu_naicu_panel.svg",
}

# Nombre canónico del CSV principal de resultados
_MAIN_CSV = "nairu_colombia.csv"
_MAIN_SUMMARY = "nairu_summary.txt"


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


def _rewrite_v5_to_final(text: str) -> str:
    return (
        text
        .replace("Estimation v5", "Estimation NAIRU/NAICU")
        .replace("nairu_naicu_panel_v5.png", "nairu_naicu_panel.png")
        .replace("nairu_naicu_panel_v5.svg", "nairu_naicu_panel.svg")
    )


def _copy_outputs(src_dir: Path, dst_dir: Path) -> None:
    """Copia y renombra outputs v5 → nombres finales del repo."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    for v5_name, final_name in _V5_TO_FINAL.items():
        src_file = src_dir / v5_name
        dst_file = dst_dir / final_name
        if not src_file.exists():
            logger.warning("[NAIRU] Archivo no encontrado para copiar: %s", src_file)
            continue
        if v5_name.endswith(".txt"):
            text = src_file.read_text(encoding="utf-8")
            dst_file.write_text(_rewrite_v5_to_final(text), encoding="utf-8")
        else:
            shutil.copy2(src_file, dst_file)
    logger.info("[NAIRU] Outputs copiados a %s", dst_dir)


def _needs_estimation(data_path: Path, existing_csv: Path, package_dir: Path) -> bool:
    """Decide si toca (re)estimar, o si el CSV existente ya está al día.

    Fix 2026-09-01 (auditoria_src_2026-08-21.md): antes solo se comparaba
    el mtime de ``Data_NAIRU.xlsx`` contra el CSV de salida -- un cambio en
    el CÓDIGO del modelo (``model_core.py``, ``estimation.py``) no disparaba
    re-estimación aunque cambiara el resultado. Mordía justo al arreglar el
    off-by-one del suavizador de Kalman: sin este fix, corregir
    ``model_core.py`` no habría bastado para que se regenerara
    ``nairu_colombia.csv``. Ahora también se compara el mtime más reciente
    de los ``.py`` de ``package_dir`` (el paquete ``src/nairu/`` completo:
    este archivo + ``model_core.py``, y cualquiera que se agregue a futuro).

    Parameters
    ----------
    data_path:
        Excel de entrada (``Data_NAIRU.xlsx``).
    existing_csv:
        CSV de salida ya publicado (``nairu_colombia.csv``), si existe.
    package_dir:
        Directorio a inspeccionar en busca de ``.py`` más nuevos que el CSV
        (en producción, ``Path(__file__).resolve().parent`` = ``src/nairu/``;
        en tests, un ``tmp_path`` con archivos sintéticos).
    """
    if not existing_csv.exists():
        return True
    csv_mtime = existing_csv.stat().st_mtime
    if data_path.stat().st_mtime > csv_mtime:
        return True
    source_mtime = max(p.stat().st_mtime for p in package_dir.glob("*.py"))
    return source_mtime > csv_mtime


# ═══════════════════════════════════════════════════════════════════════
# API pública
# ═══════════════════════════════════════════════════════════════════════

def build_outputs(
    data_path: Path | None = None,
    output_dir: Path | None = None,
) -> str:
    """Ejecuta la estimación NAIRU/NAICU y escribe los outputs.

    Estrategia
    ----------
    * Si ``Data_NAIRU.xlsx`` es más nuevo que ``nairu_colombia.csv``
      (o este no existe) → re-estima con los datos actualizados.
    * Si algún ``.py`` de ``src/nairu/`` (el modelo o este orquestador) es
      más nuevo que ``nairu_colombia.csv`` → también re-estima (fix
      2026-09-01: antes un cambio de código nunca disparaba re-estimación).
    * Si ``nairu_colombia.csv`` ya existe y es más reciente que ambos →
      omite la re-estimación.

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

    output_dir.mkdir(parents=True, exist_ok=True)
    existing_csv = output_dir / _MAIN_CSV

    # ── Decidir si re-estimar ─────────────────────────────────────────
    needs_estimation = _needs_estimation(
        data_path=data_path,
        existing_csv=existing_csv,
        package_dir=Path(__file__).resolve().parent,
    )

    if not needs_estimation:
        summary_path = output_dir / _MAIN_SUMMARY
        summary = (
            summary_path.read_text(encoding="utf-8")
            if summary_path.exists() else
            f"[NAIRU] Resultados en {output_dir} están al día — se omite re-estimación."
        )
        logger.info(
            "[NAIRU] %s está actualizado — se omite re-estimación.", existing_csv.name
        )
        return summary

    # ── Ruta 2: correr el modelo ──────────────────────────────────────
    tmp_dir = output_dir / "_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    logger.info("[NAIRU] Cargando modelo core desde %s …", MODEL_CORE_PATH)
    module = _load_model_core()

    logger.info("[NAIRU] Cargando y preparando datos desde %s …", data_path)
    data = module.load_and_prepare_data(data_path)

    logger.info("[NAIRU] Construyendo dataset del modelo …")
    model_data = module.build_model_data(data)

    logger.info("[NAIRU] Estimando parámetros (MLE) …")
    fit = module.estimate_parameters(model_data)

    logger.info("[NAIRU] Generando outputs en %s …", tmp_dir)
    try:
        _, summary_v5 = module.build_outputs(data, model_data, fit, tmp_dir)
        summary = _rewrite_v5_to_final(summary_v5)

        _copy_outputs(tmp_dir, output_dir)
        (output_dir / _MAIN_SUMMARY).write_text(summary, encoding="utf-8")

        logger.info("[NAIRU] Estimación completada. Outputs en %s", output_dir)
        return summary
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)


def run() -> str:
    """Entry-point para el pipeline principal."""
    return build_outputs()
