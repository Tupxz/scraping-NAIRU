"""Descarga y parseo de series historicas de PIB trimestral del DANE.

Provee dos series historicas para empalme con la serie actual (Base 2015):

  Base 2005  (2000Q1 - 2011Q2, 46 obs)
  Base 1994  (1994Q1 - 2007Q4, 56 obs)

Cadena de empalme (growth-rate splice):
  Base 2015  <-  Base 2005  <-  Base 1994
  Resultado final: 1994Q1 - presente (~120 obs)
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

from src.sources.dane.common import dane_request_kwargs

logger = logging.getLogger("nairu_pipeline.dane.gdp_hist")

_URL_BASE2005 = (
    "https://www.dane.gov.co/files/investigaciones/pib/trimestrales/PIB_2011-2.zip"
)
_XLS_INSIDE_ZIP_B2005 = (
    "PIB_2011-2/1.1 Cuadros_Oferta_Constantes_desestacionalizado SV.xls"
)
_SHEET_B2005     = " Grandes Ramas"
_COL_YEAR_B2005  = 1
_COL_QTR_B2005   = 2
_COL_PIB_B2005   = 18
_LEVEL_MIN_B2005 = 1000

_URL_BASE1994 = (
    "https://www.dane.gov.co/files/investigaciones/pib/trimestrales/"
    "PIB_Itrim08/base_1994.zip"
)
_XLS_INSIDE_ZIP_B1994 = (
    "internet/Pib Oferta constante con ilicitos 1994-I a 2007-IV "
    "(desestacionalizadas).xls"
)
_SHEET_B1994    = "Cuadro No.1- Valores absolutos"
_PIB_ROW_B1994  = 77
_YEAR_ROW_B1994 = 9
_QTR_ROW_B1994  = 10
_DATA_COL_START = 2

_ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4}


def _download_zip_extract_xls(
    url: str,
    xls_inside_zip: str,
    output_path: Path,
    timeout: int = 30,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        logger.info("[GDP-hist] Archivo ya existe: %s", output_path)
        return output_path
    logger.info("[GDP-hist] Descargando: %s", url)
    resp = requests.get(url, **dane_request_kwargs(timeout=timeout))
    resp.raise_for_status()
    logger.info("[GDP-hist] Descargado: %.1f KB", len(resp.content) / 1024)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        data = zf.read(xls_inside_zip)
    output_path.write_bytes(data)
    logger.info("[GDP-hist] XLS guardado: %s", output_path)
    return output_path


def download_gdp_historical(output_path: Path, timeout: int = 30) -> Path:
    """Descarga la serie Base 2005 (2000Q1-2011Q2)."""
    return _download_zip_extract_xls(
        _URL_BASE2005, _XLS_INSIDE_ZIP_B2005, output_path, timeout
    )


def parse_gdp_historical(xls_path: Path) -> pd.Series:
    """Parsea el XLS Base 2005 -> pd.Series con PeriodIndex Q."""
    df = pd.read_excel(xls_path, sheet_name=_SHEET_B2005, header=None)
    quarters = df[df.iloc[:, _COL_QTR_B2005].isin(_ROMAN)].copy()
    quarters[_COL_YEAR_B2005] = quarters[_COL_YEAR_B2005].ffill()
    quarters[_COL_YEAR_B2005] = (
        quarters[_COL_YEAR_B2005]
        .astype(str).str.extract(r"(\d{4})")[0].astype(int)
    )
    quarters[_COL_PIB_B2005] = pd.to_numeric(quarters[_COL_PIB_B2005], errors="coerce")
    levels = quarters[quarters[_COL_PIB_B2005] > _LEVEL_MIN_B2005].copy()
    levels["_q_num"] = levels[_COL_QTR_B2005].map(_ROMAN)
    periods = pd.PeriodIndex.from_fields(
        year=levels[_COL_YEAR_B2005].values,
        quarter=levels["_q_num"].values,
        freq="Q",
    )
    series = pd.Series(levels[_COL_PIB_B2005].values, index=periods, name="gdp_base2005")
    series = series.sort_index()
    logger.info(
        "[GDP-hist] Base 2005 parseada: %d trimestres (%s - %s)",
        len(series), series.index[0], series.index[-1],
    )
    return series


def download_gdp_base1994(output_path: Path, timeout: int = 30) -> Path:
    """Descarga la serie Base 1994 (1994Q1-2007Q4)."""
    return _download_zip_extract_xls(
        _URL_BASE1994, _XLS_INSIDE_ZIP_B1994, output_path, timeout
    )


def parse_gdp_base1994(xls_path: Path) -> pd.Series:
    """Parsea el XLS Base 1994 -> pd.Series con PeriodIndex Q.

    Fila 9  = anos  (forward-filled)
    Fila 10 = trimestres (I/II/III/IV/Anual)
    Fila 77 = PRODUCTO INTERNO BRUTO total
    """
    df = pd.read_excel(xls_path, sheet_name=_SHEET_B1994, header=None)
    year_row     = df.iloc[_YEAR_ROW_B1994, _DATA_COL_START:].ffill()
    qtr_row      = df.iloc[_QTR_ROW_B1994,  _DATA_COL_START:]
    pib_row_vals = df.iloc[_PIB_ROW_B1994,   _DATA_COL_START:]

    records = []
    for year_val, q_val, pib_val in zip(year_row, qtr_row, pib_row_vals):
        if q_val not in _ROMAN:
            continue
        try:
            year = int(float(year_val))
            pib  = float(pib_val)
        except (ValueError, TypeError):
            continue
        records.append({"year": year, "quarter": _ROMAN[q_val], "gdp": pib})

    rec_df  = pd.DataFrame(records)
    periods = pd.PeriodIndex.from_fields(
        year=rec_df["year"].values,
        quarter=rec_df["quarter"].values,
        freq="Q",
    )
    series = pd.Series(rec_df["gdp"].values, index=periods, name="gdp_base1994")
    series = series.sort_index()
    logger.info(
        "[GDP-hist] Base 1994 parseada: %d trimestres (%s - %s)",
        len(series), series.index[0], series.index[-1],
    )
    return series


def splice_series(new: pd.Series, old: pd.Series) -> pd.Series:
    """Empalme por encadenamiento: extiende *new* hacia atras con *old*.

    La escala de *old* se convierte a la de *new* usando el ratio promedio
    en el periodo de traslape.
    """
    overlap_start = max(new.index[0], old.index[0])
    overlap_end   = min(new.index[-1], old.index[-1])

    if overlap_start > overlap_end:
        raise ValueError(
            f"Sin traslape: new {new.index[0]}-{new.index[-1]}, "
            f"old {old.index[0]}-{old.index[-1]}"
        )

    overlap_new = new[overlap_start:overlap_end]
    overlap_old = old[overlap_start:overlap_end].reindex(overlap_new.index)
    ratio = (overlap_new / overlap_old).mean()

    logger.info(
        "[GDP-hist] Traslape: %s - %s (%d obs), ratio=%.6f",
        overlap_start, overlap_end, len(overlap_new), ratio,
    )

    pre = old[old.index < new.index[0]]
    if len(pre) == 0:
        logger.warning("[GDP-hist] Sin periodos anteriores; devolviendo serie sin cambios.")
        return new

    result = pd.concat([pre * ratio, new]).sort_index()
    logger.info(
        "[GDP-hist] Serie empalmada: %d trimestres (%s - %s)",
        len(result), result.index[0], result.index[-1],
    )
    return result
