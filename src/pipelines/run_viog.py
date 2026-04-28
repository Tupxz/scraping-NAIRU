"""Pipeline runner para el VIOG (output gap USA ponderado por filtros)."""

from __future__ import annotations

from src.config import OUTPUTS_DIR, PROCESSED_DIR, VIOG_CONFIG
from src.sources.viog.viog import run_viog_pipeline


def run() -> None:
    input_path = PROCESSED_DIR / VIOG_CONFIG.input_filename
    output_path = PROCESSED_DIR / VIOG_CONFIG.processed_filename
    plot_dir = OUTPUTS_DIR / "viog"
    df = run_viog_pipeline(input_path, output_path, plot=True, plot_dir=plot_dir)
    print(f"[VIOG] {len(df)} observaciones guardadas en {output_path}")
    print(f"[VIOG] Gráficas guardadas en {plot_dir}")
