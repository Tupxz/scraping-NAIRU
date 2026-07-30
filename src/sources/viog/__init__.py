"""Fuente VIOG — Brecha del producto ponderada multi-filtro.

El método VIOG combina 5 filtros de tendencia (BK, CF, BW, HP, Kalman)
más una serie de referencia exógena para construir una brecha del
producto robusta a la elección del filtro.
"""

from src.sources.viog.viog import (
    apply_filters,
    cf_filter_one_sided,
    compute_gaps,
    compute_viog_weights,
    compute_weighted_gap,
    load_series,
    plot_filters,
    run_viog_pipeline,
)

__all__ = [
    "apply_filters",
    "cf_filter_one_sided",
    "compute_gaps",
    "compute_viog_weights",
    "compute_weighted_gap",
    "load_series",
    "plot_filters",
    "run_viog_pipeline",
]
