"""Pipeline runner para el VIOG (output gap USA ponderado por filtros)."""

from __future__ import annotations

from src.config import INPUTS_DIR, OUTPUTS_DIR, PROCESSED_DIR, VIOG_CONFIG
from src.sources.viog.viog import run_viog_pipeline


def run() -> None:
    # Input: archivo manual en data/inputs/ (no proviene de un scraper).
    # Output: CSV procesado en data/processed/ (alimenta al merge).
    input_path = INPUTS_DIR / VIOG_CONFIG.input_filename
    output_path = PROCESSED_DIR / VIOG_CONFIG.processed_filename
    plot_dir = OUTPUTS_DIR / "viog"
    df = run_viog_pipeline(input_path, output_path, plot=True, plot_dir=plot_dir)
    print(f"[VIOG] {len(df)} observaciones guardadas en {output_path}")
    print(f"[VIOG] Gráficas guardadas en {plot_dir}")
