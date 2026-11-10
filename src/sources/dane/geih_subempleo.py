"""Subocupación y tasas brutas de la GEIH (anexo principal) para v7.

``geih_subempleo_bruto_mensual.csv``: del anexo **principal** de la GEIH
(``anex-GEIH-<mes><año>.xlsx``, series sin desestacionalizar), hoja "Total
nacional", primeras filas del bloque Total Nacional:

- ``tasa_subocupacion``          ← Tasa de Subocupación (TS)
- ``tcsd_subempleo_subjetivo``   ← TCSD (tasa combinada de subocupación y desocupación)
- ``tcdftp_subempleo_objetivo``  ← TCDFTP (tasa combinada de desocupación y fuerza de
  trabajo potencial; 0 antes de que el DANE la publicara)
- ``td_bruta``, ``tgp_bruta``, ``to_bruta`` ← TD, TGP, TO brutas

Los nombres de columna son los del CSV de la línea base (v7 usa
``tasa_subocupacion``, en variación interanual).  6 decimales.

Mar–jul 2020: el DANE no midió la población subocupada (nota del anexo) y
publica 0; aquí esas tres tasas quedan en NaN y la fila se conserva (el CSV
del compañero omite esas 5 filas; para v7 es equivalente).  TCDFTP vale 0
antes de que el DANE la calculara; se deja como viene, igual que en la línea
base (v7 no la usa).  Se reutiliza
el parser del anexo desestacionalizado (``unemployment.load_geih_excel``):
mismo layout de años × meses.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from src.config import GEIH_BRUTA_CONFIG, PROCESSED_DIR, RAW_DANE_DIR, SUBEMPLEO_COLUMNS, GEIHConfig
from src.io_utils import save_csv
from src.sources.dane.common import make_dane_session
from src.sources.dane.unemployment import (
    download_geih_excel,
    extract_geih_xlsx_links,
    fetch_geih_page,
    load_geih_excel,
    select_geih_link,
)

logger = logging.getLogger("nairu_pipeline.dane.geih_subempleo")

SERIES = SUBEMPLEO_COLUMNS[1:7]


def parsear_subempleo(xlsx_path: Path, config: GEIHConfig = GEIH_BRUTA_CONFIG,
                      hoy: date | None = None) -> pd.DataFrame:
    """Series brutas del bloque Total Nacional, 6 decimales."""
    df = load_geih_excel(xlsx_path, config)
    faltan = [c for c in SERIES if c not in df.columns]
    if faltan:
        raise ValueError(f"El anexo principal GEIH no trae: {faltan}")
    out = df[["date"] + SERIES].copy()
    out[SERIES] = out[SERIES].astype(float).round(6)
    # Mar–jul 2020 ("Mar*"…"Jul*"): el DANE no pudo medir la población
    # subocupada por el cambio de operativo en la pandemia y publica 0 como
    # relleno.  TS = 0 es imposible a nivel nacional → NaN en las tres tasas de
    # subocupación (TD/TGP/TO brutas sí son válidas y se conservan).
    sin_dato = out["tasa_subocupacion"] == 0
    if sin_dato.any():
        logger.info("Subocupación no medida (TS = 0 → NaN): %s",
                    out.loc[sin_dato, "date"].dt.strftime("%Y-%m").tolist())
    out.loc[sin_dato, ["tasa_subocupacion", "tcsd_subempleo_subjetivo",
                       "tcdftp_subempleo_objetivo"]] = float("nan")
    out["source"] = "DANE_GEIH_anexo_principal"
    out["download_date"] = (hoy or date.today()).isoformat()
    if out["date"].duplicated().any():
        raise ValueError("Meses duplicados en el anexo principal GEIH.")
    return out[SUBEMPLEO_COLUMNS].sort_values("date").reset_index(drop=True)


def construir_subempleo(config: GEIHConfig = GEIH_BRUTA_CONFIG,
                        raw_dir: Path = RAW_DANE_DIR,
                        session: requests.Session | None = None,
                        hoy: date | None = None) -> pd.DataFrame:
    """Página del DANE → anexo principal más reciente → series (sin escribir)."""
    session = session or make_dane_session(headers=config.http_headers)
    html = fetch_geih_page(config, session=session)
    destino = select_geih_link(extract_geih_xlsx_links(html, config))
    xlsx = download_geih_excel(url=destino["url"], output_dir=raw_dir, config=config, session=session)
    df = parsear_subempleo(xlsx, config, hoy)
    logger.info("Subempleo GEIH (%s): %d meses, %s → %s", destino["url"].rsplit("/", 1)[-1],
                len(df), df["date"].min().date(), df["date"].max().date())
    return df


def guardar_subempleo(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR,
                      config: GEIHConfig = GEIH_BRUTA_CONFIG) -> Path:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path = output_dir / config.processed_filename
    save_csv(out, path)
    logger.info("Subempleo GEIH guardado: %s (%d filas)", path, len(out))
    return path
