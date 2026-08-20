"""Valida la corrección del filtro Kalman/UCM (v0.4.2) sin tocar outputs publicados.

Compara la brecha Kalman *vieja* (la de los CSV publicados en data/processed/,
generados con ciclo determinístico) contra la *nueva* (recalculada con la
especificación Stata-like: rwdrift + ciclo estocástico amortiguado), usando la
brecha BHP como referencia visual.

Escribe todo en outputs/validation_kalman/ (CSV + PNG por serie). No modifica
data/processed/ ni docs/data/ — para regenerar los oficiales usa:
    python -m src.main --viog        # USA
    python -m src.main --viog-co     # Colombia

Uso:
    python scripts/validate_kalman_fix.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import INPUTS_DIR, PROCESSED_DIR, VIOG_CO_CONFIG, VIOG_CONFIG  # noqa: E402
from src.sources.viog.viog import run_viog_pipeline  # noqa: E402

OUT_DIR = ROOT / "outputs" / "validation_kalman"


def validate(label: str, cfg) -> None:
    in_path = INPUTS_DIR / cfg.input_filename
    old_path = PROCESSED_DIR / cfg.processed_filename
    if not in_path.exists():
        print(f"[{label}] SALTADO — no existe {in_path}")
        return

    print(f"[{label}] Recalculando con la nueva especificación...")
    new = run_viog_pipeline(
        input_path=in_path,
        output_path=OUT_DIR / f"nuevo_{cfg.processed_filename}",
        series_col=cfg.series_col,
        ref_col=cfg.ref_col,
        source_label=f"validación {label}",
        plot=False,
    )
    new["date"] = pd.to_datetime(new["date"])

    fig, ax = plt.subplots(figsize=(11, 5))
    if old_path.exists():
        old = pd.read_csv(old_path, parse_dates=["date"])
        ax.plot(old["date"], old["gap_kalman"], color="firebrick", alpha=0.6,
                label="gap_kalman VIEJO (ciclo determinístico)")
        print(f"[{label}] |gap| máx viejo: {old['gap_kalman'].abs().max():.3f}")
    ax.plot(new["date"], new["gap_kalman"], color="steelblue", lw=2,
            label="gap_kalman NUEVO (rwdrift + ciclo estocástico)")
    ax.plot(new["date"], new["gap_bhp"], color="gray", ls=":",
            label="gap_bhp (referencia)")
    ax.axhline(0, color="black", lw=0.8, ls="--")
    ax.set_ylim(-0.20, 0.20)
    ax.set_title(f"Brecha Kalman/UCM — {label}: especificación vieja vs nueva")
    ax.set_ylabel("Brecha (log)")
    ax.legend(fontsize="small")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    png = OUT_DIR / f"kalman_antes_despues_{label.lower()}.png"
    fig.savefig(png, dpi=150)
    plt.close(fig)

    corr = new["gap_kalman"].corr(new["gap_bhp"])
    print(f"[{label}] |gap| máx nuevo: {new['gap_kalman'].abs().max():.3f}")
    print(f"[{label}] corr(gap_kalman, gap_bhp) nuevo: {corr:.2f}")
    print(f"[{label}] figura: {png.relative_to(ROOT)}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    validate("USA", VIOG_CONFIG)
    validate("Colombia", VIOG_CO_CONFIG)
    print("\nSi las brechas nuevas son de ciclo de negocio (±2-6%) y siguen a la BHP")
    print("sin sinusoides ni transitorios, regenera los oficiales con --viog / --viog-co.")


if __name__ == "__main__":
    main()
