"""Tasas desestacionalizadas de la GEIH para v7 (``geih_tgp_to_td_desest_mensual.csv``).

Salen del mismo anexo GEIH desestacionalizado que ``unemployment.py`` (se
escriben en ``--unemployment``, sin otra descarga):

- ``tgp_desest`` = Tasa Global de Participación
- ``to_desest``  = Tasa de Ocupación
- ``td_desest``  = Tasa de Desocupación con precisión completa
  (= ``unemployment_rate_calc``, desocupados/FL; no la TD publicada a 1 decimal)

con 6 decimales, como el CSV de la línea base.  Los meses del empalme
ECH→GEIH (jul–ago 2006, "Jul*"/"Ago*" en el anexo) quedan en **NaN**: v7 los
trata como faltantes en la TGP (escalón ``tgp_empalme_geih``), no se
interpolan.  El CSV del compañero (anexo de jul-2026) es idéntico al que sale
de ese anexo con esta regla (las filas de jul–ago 2006 allá no existen; aquí
existen con NaN, que v7 lee igual).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import GEIH_CONFIG, PROCESSED_DIR, GEIHConfig
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.dane.geih_tasas")

COLUMNAS = ["date", "tgp_desest", "to_desest", "td_desest", "source", "download_date"]


def construir_tasas_desest(labor: pd.DataFrame, config: GEIHConfig = GEIH_CONFIG) -> pd.DataFrame:
    """Tasas desestacionalizadas desde el DataFrame limpio de la GEIH."""
    faltan = {"tgp_rate", "to_rate", "unemployment_rate_calc"} - set(labor.columns)
    if faltan:
        raise ValueError(f"El anexo GEIH no trae las series necesarias para v7: {sorted(faltan)}")
    out = pd.DataFrame({
        "date": pd.to_datetime(labor["date"]).to_numpy(),
        "tgp_desest": labor["tgp_rate"].astype(float).round(6).to_numpy(),
        "to_desest": labor["to_rate"].astype(float).round(6).to_numpy(),
        "td_desest": labor["unemployment_rate_calc"].astype(float).round(6).to_numpy(),
        "source": labor["source"].to_numpy() if "source" in labor else "DANE",
        "download_date": labor["download_date"].to_numpy() if "download_date" in labor else None,
    })
    empalme = out["date"].isin(pd.to_datetime(list(config.meses_empalme_nan)))
    out.loc[empalme, ["tgp_desest", "to_desest", "td_desest"]] = float("nan")
    return out[COLUMNAS].sort_values("date").reset_index(drop=True)


def guardar_tasas(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR,
                  config: GEIHConfig = GEIH_CONFIG) -> Path:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path = output_dir / config.tasas_filename
    save_csv(out, path)
    logger.info("Tasas GEIH desestacionalizadas guardadas: %s (%d filas)", path, len(out))
    return path
