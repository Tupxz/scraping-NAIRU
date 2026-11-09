"""Valida el extractor del ICU (informes EOIC) contra el histórico de la ANDI.

Uso:
    python -m scripts.validar_icu_eoic [carpeta_pdf]

Corre ``src.sources.andi.eoic.validar_parser_contra_historico`` sobre los PDF
de ``data/raw/andi/`` (o la carpeta dada), imprime el error medio y el máximo
frente a ``data/inputs/icu_historical.csv`` y los meses donde falla, y guarda
el detalle en ``outputs/andi/validacion_parser_icu.csv``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from src.config import OUTPUTS_DIR, RAW_ANDI_DIR
from src.sources.andi.eoic import resumen_validacion, validar_parser_contra_historico


def main() -> None:
    carpeta = Path(sys.argv[1]) if len(sys.argv) > 1 else RAW_ANDI_DIR
    df = validar_parser_contra_historico(carpeta)
    r = resumen_validacion(df)
    salida = OUTPUTS_DIR / "andi" / "validacion_parser_icu.csv"
    salida.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(salida, index=False)

    print(f"Informes únicos: {r['informes']}  por estado: {r['por_estado']}")
    print(f"Comparados con el histórico: {r['comparados']}  "
          f"(|e| ≤ 0,05 = redondeo a 1 decimal: {r['dentro_redondeo']})")
    print(f"Error medio |e| = {r['error_medio_abs']:.4f}  mediano = {r['error_mediano_abs']:.4f}  "
          f"máximo = {r['error_max_abs']:.4f} ({r['mes_error_max']})")
    with pd.option_context("display.width", 200, "display.max_colwidth", 90):
        fallas = df[df["estado"] != "ok"][["archivo", "mes", "estado", "motivo"]]
        if len(fallas):
            print("\nFallas:\n" + fallas.to_string(index=False))
        grandes = df[df["error"].abs() > 0.2][["mes", "valor", "historico", "error"]]
        if len(grandes):
            print("\n|e| > 0,2:\n" + grandes.to_string(index=False))
    print(f"\nDetalle: {salida}")


if __name__ == "__main__":
    main()
