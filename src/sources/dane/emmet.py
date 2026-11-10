"""Salarios de la industria (DANE, EMMET) para v7.

Replica ``legacy/v7/construir_salarios_emmet.py`` con el anexo vigente:

1. **DESCARGA** — En la página de la EMMET se busca el anexo
   ``anex-EMMET-TotalNacional-<mes><año>.xlsx`` más reciente.
2. **PARSING** — Hoja "9. Enlace legal hasta 2014", fila ``T_IND`` (total
   industria, base 2018=100): "Total Sueldos" (índice **real**; el anexo
   territorial lo rotula así), "Total Empleo" y "Horas Totales".
3. **CÁLCULO** — Con el IPC total (``data/processed/ipc_colombia.csv``, el
   mismo índice del Banco de la República que usó el compañero: idéntico en
   los 284 meses comunes):

       salario_real_por_ocupado    = 100 · idx_sueldos_real / idx_empleo
       salario_nominal_por_ocupado = salario_real_por_ocupado · ipc / 100
       var_anual_x                 = 100 · (x_t / x_{t−12} − 1)

Salida: ``data/processed/emmet_salarios_v7.csv`` con las columnas del CSV de
la línea base (+ ``source``, ``download_date``).  v7 usa
``var_anual_salario_nominal``.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from src.config import (
    EMMET_CONFIG,
    EMMET_PROCESSED_COLUMNS,
    PROCESSED_DIR,
    RAW_DANE_DIR,
    EMMETConfig,
)
from src.io_utils import save_csv
from src.sources.dane.common import MONTH_ABBR_ES, make_dane_session

logger = logging.getLogger("nairu_pipeline.dane.emmet")

FUENTE = "DANE_EMMET+IPC"


# ═══════════════════════════════════════════════════════════════════════
# Descarga
# ═══════════════════════════════════════════════════════════════════════


def seleccionar_anexo(html: str, config: EMMETConfig = EMMET_CONFIG) -> str:
    """URL absoluta del anexo Total Nacional más reciente de la página."""
    patron = re.compile(config.link_pattern, re.IGNORECASE)
    candidatos: list[tuple[int, int, str]] = []
    for href in set(re.findall(r'href="([^"]+)"', html)):
        m = patron.search(href)
        if m and m.group(1).lower() in MONTH_ABBR_ES:
            url = href if href.startswith("http") else config.base_url + href
            candidatos.append((int(m.group(2)), MONTH_ABBR_ES[m.group(1).lower()], url))
    if not candidatos:
        raise ValueError("No se encontró el anexo EMMET Total Nacional en la página del DANE.")
    return max(candidatos)[2]


def descargar_emmet(config: EMMETConfig = EMMET_CONFIG,
                    session: requests.Session | None = None) -> tuple[bytes, str]:
    session = session or make_dane_session(headers=config.http_headers)
    r = session.get(config.page_url, timeout=config.timeout)
    r.raise_for_status()
    url = seleccionar_anexo(r.text, config)
    logger.info("Descargando anexo EMMET: %s", url)
    r = session.get(url, timeout=config.timeout)
    r.raise_for_status()
    if r.content[:2] != b"PK":
        raise RuntimeError(f"El anexo EMMET descargado no es un .xlsx: {url}")
    return r.content, url


# ═══════════════════════════════════════════════════════════════════════
# Parsing y cálculo
# ═══════════════════════════════════════════════════════════════════════


def leer_anexo(contenido: bytes | Path, config: EMMETConfig = EMMET_CONFIG) -> pd.DataFrame:
    """Índices del total industria: ``date, idx_sueldos_real, idx_empleo, idx_horas``."""
    fuente = io.BytesIO(contenido) if isinstance(contenido, bytes) else contenido
    try:
        raw = pd.read_excel(fuente, sheet_name=config.hoja, header=None)
    except ValueError as exc:
        raise ValueError(f"El anexo EMMET no tiene la hoja '{config.hoja}'.") from exc
    filas = raw.index[raw.iloc[:, 0].astype(str).str.strip() == "Dominios"]
    if len(filas) == 0:
        raise ValueError(f"No se encontró la fila 'Dominios' en '{config.hoja}'.")
    fila_enc = filas[0]
    enc = [" ".join(str(c).split()) for c in raw.iloc[fila_enc]]   # el anexo trae dobles espacios
    datos = raw.iloc[fila_enc + 1:]
    datos = datos[datos.iloc[:, 0].astype(str).str.strip() == config.dominio]
    if datos.empty:
        raise ValueError(f"No hay filas '{config.dominio}' en '{config.hoja}'.")

    def col(clave: str) -> int:
        idx = [i for i, c in enumerate(enc) if clave.lower() in c.lower()]
        if not idx:
            raise ValueError(f"No se halló la columna '{clave}' en '{config.hoja}': {enc}")
        return idx[0]

    out = pd.DataFrame({
        "date": pd.to_datetime(dict(year=datos.iloc[:, 1].astype(int),
                                    month=datos.iloc[:, 2].astype(int), day=1)).to_numpy(),
        "idx_sueldos_real": pd.to_numeric(datos.iloc[:, col("Total Sueldos")], errors="coerce").to_numpy(),
        "idx_empleo": pd.to_numeric(datos.iloc[:, col("Total Empleo")], errors="coerce").to_numpy(),
        "idx_horas": pd.to_numeric(datos.iloc[:, col("Horas Totales")], errors="coerce").to_numpy(),
    })
    out = out.dropna(subset=["idx_sueldos_real"]).sort_values("date").reset_index(drop=True)
    if out["date"].duplicated().any():
        raise ValueError("Meses duplicados en el anexo EMMET (fila T_IND).")
    return out


def construir_salarios(indices: pd.DataFrame, ipc: pd.DataFrame,
                       hoy: date | None = None) -> pd.DataFrame:
    """Salario real y nominal por ocupado y sus variaciones anuales (como el compañero).

    ``ipc``: columnas ``date`` y ``ipc_index`` (o ``ipc``), mensual.
    """
    ipc = ipc.rename(columns={"ipc_index": "ipc"})[["date", "ipc"]].copy()
    ipc["date"] = pd.to_datetime(ipc["date"]).dt.to_period("M").dt.to_timestamp()
    w = indices.merge(ipc, on="date", how="left")
    w["salario_real_por_ocupado"] = 100.0 * w["idx_sueldos_real"] / w["idx_empleo"]
    w["salario_nominal_por_ocupado"] = w["salario_real_por_ocupado"] * w["ipc"] / 100.0
    for c, n in (("salario_real_por_ocupado", "var_anual_salario_real"),
                 ("salario_nominal_por_ocupado", "var_anual_salario_nominal")):
        w[n] = 100.0 * (w[c] / w[c].shift(12) - 1.0)
    w["source"] = FUENTE
    w["download_date"] = (hoy or date.today()).isoformat()
    return w[EMMET_PROCESSED_COLUMNS]


def construir_emmet(config: EMMETConfig = EMMET_CONFIG,
                    raw_dir: Path | None = RAW_DANE_DIR,
                    ipc_path: Path | None = None,
                    session: requests.Session | None = None,
                    hoy: date | None = None) -> pd.DataFrame:
    """Descarga → crudo → índices → salarios (sin escribir el procesado)."""
    contenido, url = descargar_emmet(config, session)
    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / config.raw_filename).write_bytes(contenido)
    indices = leer_anexo(contenido, config)
    ipc = pd.read_csv(ipc_path or (PROCESSED_DIR / "ipc_colombia.csv"))
    df = construir_salarios(indices, ipc, hoy)
    logger.info("EMMET (%s): %d meses, %s → %s; IPC hasta %s", url.rsplit("/", 1)[-1], len(df),
                df["date"].min().date(), df["date"].max().date(),
                df.dropna(subset=["ipc"])["date"].max().date())
    return df


def guardar_emmet(df: pd.DataFrame, output_dir: Path = PROCESSED_DIR,
                  config: EMMETConfig = EMMET_CONFIG) -> Path:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path = output_dir / config.processed_filename
    save_csv(out, path)
    logger.info("EMMET guardada: %s (%d filas)", path, len(out))
    return path
