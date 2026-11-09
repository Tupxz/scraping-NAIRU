"""TRM (Tasa Representativa del Mercado, COP por USD) desde datos.gov.co.

Pipeline de 3 capas:

1. **DESCARGA** — Consulta paginada (``$limit``/``$offset``, orden por
   ``vigenciadesde``) del conjunto Socrata ``32sa-8pi3`` publicado por la
   Superintendencia Financiera.  Se compara el total descargado contra
   ``count(*)`` del portal: si no coincide, la descarga se considera
   truncada y se lanza un error.
2. **PARSING** — Lee el CSV crudo (``valor, unidad, vigenciadesde,
   vigenciahasta``) y verifica que las vigencias cubran todos los días
   calendario sin huecos ni traslapes.
3. **AGREGACIÓN** — Promedio mensual de la TRM (ver ``TRMConfig``):

   - ``"dias_calendario"`` (por defecto): cada registro se expande a los
     días de su vigencia y se promedian los días calendario del mes.  Un
     registro vigente del sábado 31-oct al martes 3-nov aporta 1 día a
     octubre y 3 a noviembre.
   - ``"registro"``: promedio simple de los registros cuya
     ``vigenciadesde`` cae en el mes (ese mismo registro cuenta entero
     para octubre).

   Solo se publican meses completos (todos sus días con TRM vigente).

Salida: ``data/processed/trm_cop_usd_promedio_mensual.csv`` con
``date, year, month, trm_cop_usd, source, download_date`` y, para que
``legacy/v7/pib_potencial_integrado_v7.py`` corra tal cual, una copia con
solo ``date, trm_cop_usd`` en ``legacy/v7/``.
"""

from __future__ import annotations

import io
import logging
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import (
    PROCESSED_DIR,
    PROJECT_ROOT,
    RAW_SFC_DIR,
    TRM_CONFIG,
    TRM_PROCESSED_COLUMNS,
    TRMConfig,
)
from src.io_utils import save_csv

logger = logging.getLogger("nairu_pipeline.sfc.trm")

# Firma de la función que hace la petición HTTP: (url, params) -> texto.
# Se inyecta en los tests para no tocar la red.
Obtenedor = Callable[[str, dict[str, str]], str]

COLUMNAS_CRUDAS: tuple[str, ...] = ("valor", "unidad", "vigenciadesde", "vigenciahasta")
PONDERACIONES: tuple[str, ...] = ("dias_calendario", "registro")
FUENTE: str = "SFC_datos.gov.co"


# ═══════════════════════════════════════════════════════════════════════
# 1. DESCARGA
# ═══════════════════════════════════════════════════════════════════════


def obtener_http(
    url: str,
    params: dict[str, str],
    config: TRMConfig = TRM_CONFIG,
) -> str:
    """GET con reintentos (backoff exponencial); devuelve el cuerpo como texto.

    Raises
    ------
    RuntimeError
        Si fallan todos los intentos.
    """
    import requests

    ultimo_error: Exception | None = None
    for intento in range(1, config.max_retries + 1):
        try:
            resp = requests.get(
                url, params=params, headers=config.http_headers,
                timeout=config.timeout,
            )
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except requests.RequestException as exc:
            ultimo_error = exc
            logger.warning(
                "Intento %d/%d falló (%s)", intento, config.max_retries, exc,
            )
            if intento < config.max_retries:
                time.sleep(2 ** intento)
    raise RuntimeError(
        f"No se pudo descargar la TRM de {url} tras {config.max_retries} "
        f"intentos. Último error: {ultimo_error}"
    )


def _obtenedor_por_defecto(config: TRMConfig) -> Obtenedor:
    return lambda url, params: obtener_http(url, params, config)


def contar_registros(
    config: TRMConfig = TRM_CONFIG,
    obtener: Obtenedor | None = None,
) -> int:
    """Número total de registros que reporta el portal (``count(*)``)."""
    obtener = obtener or _obtenedor_por_defecto(config)
    texto = obtener(config.endpoint_url, {"$select": "count(*)"})
    df = pd.read_csv(io.StringIO(texto))
    if df.shape != (1, 1):
        raise ValueError(f"Respuesta inesperada a count(*): {texto[:200]!r}")
    return int(df.iloc[0, 0])


def descargar_trm(
    config: TRMConfig = TRM_CONFIG,
    obtener: Obtenedor | None = None,
) -> str:
    """Descarga todos los registros de la TRM, página por página.

    Returns
    -------
    str
        Un único CSV (encabezado + todas las filas) en el orden del portal.

    Raises
    ------
    RuntimeError
        Si el total descargado no coincide con ``count(*)`` o si una
        página trae un encabezado distinto.
    """
    obtener = obtener or _obtenedor_por_defecto(config)
    total = contar_registros(config, obtener)
    if total <= 0:
        raise RuntimeError("El portal reporta 0 registros de TRM.")
    logger.info("El portal reporta %d registros de TRM", total)

    encabezado: str | None = None
    filas: list[str] = []
    max_paginas = total // config.page_size + 2   # guarda contra bucles
    for pagina in range(max_paginas):
        params = {
            "$order": "vigenciadesde,:id",
            "$limit": str(config.page_size),
            "$offset": str(pagina * config.page_size),
        }
        lineas = obtener(config.endpoint_url, params).strip().splitlines()
        if not lineas:
            raise RuntimeError(f"Página {pagina} vacía (sin encabezado).")
        if encabezado is None:
            encabezado = lineas[0]
        elif lineas[0] != encabezado:
            raise RuntimeError(
                f"Encabezado distinto en la página {pagina}: "
                f"{lineas[0]!r} != {encabezado!r}"
            )
        cuerpo = lineas[1:]
        filas.extend(cuerpo)
        logger.info("Página %d: %d filas", pagina, len(cuerpo))
        if len(cuerpo) < config.page_size:
            break

    if len(filas) != total:
        raise RuntimeError(
            f"Descarga de la TRM truncada o inconsistente: {len(filas)} "
            f"filas descargadas vs {total} reportadas por count(*)."
        )
    return "\n".join([encabezado, *filas]) + "\n"


# ═══════════════════════════════════════════════════════════════════════
# 2. PARSING
# ═══════════════════════════════════════════════════════════════════════


def parsear_trm_csv(
    csv_text: str,
    config: TRMConfig = TRM_CONFIG,
) -> pd.DataFrame:
    """Parsea el CSV del portal.

    Returns
    -------
    pd.DataFrame
        ``vigenciadesde``, ``vigenciahasta`` (datetime, sin hora) y
        ``valor`` (float), ordenado por ``vigenciadesde``.

    Raises
    ------
    ValueError
        Esquema inesperado, CSV vacío, unidad distinta de COP, valores no
        numéricos o no positivos, vigencias invertidas o duplicadas.
    """
    df = pd.read_csv(io.StringIO(csv_text), dtype=str)
    faltantes = set(COLUMNAS_CRUDAS) - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Esquema CSV de la TRM inesperado; faltan {sorted(faltantes)}. "
            f"Columnas: {list(df.columns)}"
        )
    if df.empty:
        raise ValueError("El CSV de la TRM no tiene filas.")

    unidades = set(df["unidad"].str.strip().unique())
    if unidades != {config.unidad_esperada}:
        raise ValueError(f"Unidades inesperadas en la TRM: {sorted(unidades)}")

    valor = pd.to_numeric(df["valor"], errors="coerce")
    malos = df.loc[valor.isna() | (valor <= 0), "valor"]
    if not malos.empty:
        raise ValueError(f"Valores de TRM no numéricos o <= 0: {malos.head().tolist()}")

    out = pd.DataFrame({
        "vigenciadesde": pd.to_datetime(df["vigenciadesde"]).dt.normalize(),
        "vigenciahasta": pd.to_datetime(df["vigenciahasta"]).dt.normalize(),
        "valor": valor.astype(float),
    }).sort_values("vigenciadesde").reset_index(drop=True)

    invertidas = out[out["vigenciahasta"] < out["vigenciadesde"]]
    if not invertidas.empty:
        raise ValueError(f"Vigencias con hasta < desde:\n{invertidas.head()}")
    duplicadas = out[out["vigenciadesde"].duplicated(keep=False)]
    if not duplicadas.empty:
        raise ValueError(f"vigenciadesde duplicada:\n{duplicadas.head()}")

    logger.info(
        "TRM parseada: %d registros, vigencias %s → %s",
        len(out), out["vigenciadesde"].min().date(),
        out["vigenciahasta"].max().date(),
    )
    return out


def validar_vigencias_continuas(df: pd.DataFrame) -> None:
    """Exige que cada vigencia empiece el día siguiente al fin de la anterior.

    La TRM rige todos los días calendario; un hueco o un traslape indica
    un problema de la fuente o de la descarga.

    Raises
    ------
    ValueError
        Con las primeras vigencias problemáticas.
    """
    siguiente = df["vigenciadesde"].shift(-1)
    salto = (siguiente - df["vigenciahasta"]).dt.days
    malos = df[salto.notna() & (salto != 1)]
    if not malos.empty:
        detalle = malos.assign(siguiente_desde=siguiente[malos.index])
        raise ValueError(
            f"Vigencias de la TRM no continuas ({len(malos)} casos; "
            f"hueco si siguiente_desde > hasta + 1 día, traslape si <=):\n"
            f"{detalle.head().to_string()}"
        )


def verificar_actualidad(
    df: pd.DataFrame,
    hoy: date | None = None,
    max_dias_rezago: int = TRM_CONFIG.max_dias_rezago,
) -> None:
    """Error si la última vigencia termina más de ``max_dias_rezago`` días antes de hoy."""
    hoy = hoy or date.today()
    ultima = df["vigenciahasta"].max().date()
    rezago = (hoy - ultima).days
    if rezago > max_dias_rezago:
        raise ValueError(
            f"La TRM descargada termina el {ultima} ({rezago} días antes de "
            f"{hoy}); el portal está desactualizado o la descarga se truncó."
        )


# ═══════════════════════════════════════════════════════════════════════
# 3. AGREGACIÓN MENSUAL
# ═══════════════════════════════════════════════════════════════════════


def expandir_a_dias(df: pd.DataFrame) -> pd.DataFrame:
    """Una fila por día calendario de vigencia: ``fecha``, ``trm_cop_usd``."""
    n_dias = (df["vigenciahasta"] - df["vigenciadesde"]).dt.days + 1
    rep = df.loc[df.index.repeat(n_dias)]
    desplazamiento = rep.groupby(level=0).cumcount()
    return pd.DataFrame({
        "fecha": rep["vigenciadesde"] + pd.to_timedelta(desplazamiento, unit="D"),
        "trm_cop_usd": rep["valor"],
    }).reset_index(drop=True)


def promedio_mensual(
    df: pd.DataFrame,
    ponderacion: str = TRM_CONFIG.ponderacion,
    solo_meses_completos: bool = True,
) -> pd.DataFrame:
    """Promedio mensual de la TRM.

    Parameters
    ----------
    df : pd.DataFrame
        Salida de :func:`parsear_trm_csv` (vigencias continuas).
    ponderacion : {"dias_calendario", "registro"}
        Regla de promedio (ver docstring del módulo).
    solo_meses_completos : bool
        Si ``True``, descarta los meses con algún día sin TRM vigente
        (en la práctica, el primero y el mes en curso).

    Returns
    -------
    pd.DataFrame
        ``date`` (primer día del mes), ``year``, ``month``, ``trm_cop_usd``.
    """
    if ponderacion not in PONDERACIONES:
        raise ValueError(f"ponderacion debe ser una de {PONDERACIONES}: {ponderacion!r}")

    dias = expandir_a_dias(df)
    mes_dia = dias["fecha"].dt.to_period("M")

    if ponderacion == "dias_calendario":
        valores = dias.groupby(mes_dia)["trm_cop_usd"].mean()
    else:
        mes_registro = df["vigenciadesde"].dt.to_period("M")
        valores = df.groupby(mes_registro)["valor"].mean()

    if solo_meses_completos:
        cubiertos = dias.groupby(mes_dia)["fecha"].nunique()
        completos = cubiertos.index[cubiertos == cubiertos.index.days_in_month]
        excluidos = valores.index.difference(completos)
        if len(excluidos):
            logger.info("Meses incompletos excluidos: %s", [str(p) for p in excluidos])
        valores = valores.loc[valores.index.intersection(completos)]

    if valores.empty:
        raise ValueError("No quedó ningún mes completo de TRM.")

    out = pd.DataFrame({
        "date": valores.index.to_timestamp(),
        "trm_cop_usd": valores.to_numpy(),
    })
    out["year"] = out["date"].dt.year
    out["month"] = out["date"].dt.month
    return out[["date", "year", "month", "trm_cop_usd"]].sort_values("date").reset_index(drop=True)


def limpiar_trm(
    df_mensual: pd.DataFrame,
    config: TRMConfig = TRM_CONFIG,
    hoy: date | None = None,
) -> pd.DataFrame:
    """Recorta a ``config.start_date``, redondea y agrega metadatos."""
    inicio = pd.Timestamp(config.start_date).to_period("M").to_timestamp()
    df = df_mensual[df_mensual["date"] >= inicio].copy()
    df["trm_cop_usd"] = df["trm_cop_usd"].round(4)
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["source"] = FUENTE
    df["download_date"] = (hoy or date.today()).isoformat()
    return df[TRM_PROCESSED_COLUMNS].sort_values("date").reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# GUARDADO
# ═══════════════════════════════════════════════════════════════════════


def guardar_crudo(
    csv_text: str,
    raw_dir: Path = RAW_SFC_DIR,
    config: TRMConfig = TRM_CONFIG,
) -> Path:
    """Guarda el CSV completo descargado (auditoría; gitignored)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / config.raw_csv_filename
    path.write_text(csv_text, encoding="utf-8")
    logger.info("CSV crudo guardado: %s", path)
    return path


def guardar_trm(
    df: pd.DataFrame,
    output_dir: Path = PROCESSED_DIR,
    project_root: Path | None = PROJECT_ROOT,
    config: TRMConfig = TRM_CONFIG,
) -> list[Path]:
    """Escribe el CSV procesado y la copia para ``legacy/v7/``.

    La copia solo lleva ``date`` (``YYYY-MM-DD``, primer día del mes) y
    ``trm_cop_usd``, el contrato que lee el script v7.  Si
    ``project_root`` es ``None`` o la carpeta ``legacy/v7`` no existe, la
    copia se omite (con aviso).
    """
    rutas = [save_csv(df, output_dir / config.processed_filename)]
    logger.info("TRM procesada guardada: %s", rutas[0])

    if project_root is None:
        return rutas
    copia = project_root / config.legacy_v7_copy
    if not copia.parent.is_dir():
        logger.warning("No existe %s; se omite la copia para v7.", copia.parent)
        return rutas
    df[["date", "trm_cop_usd"]].to_csv(copia, index=False, date_format="%Y-%m-%d")
    logger.info("Copia para v7 guardada: %s", copia)
    rutas.append(copia)
    return rutas


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTACIÓN (sin escritura del procesado: la hace run_trm tras validar)
# ═══════════════════════════════════════════════════════════════════════


def construir_trm_mensual(
    config: TRMConfig = TRM_CONFIG,
    raw_dir: Path | None = RAW_SFC_DIR,
    obtener: Obtenedor | None = None,
    hoy: date | None = None,
) -> pd.DataFrame:
    """Descarga → crudo → parsing → validación de vigencias → promedio mensual.

    No escribe el CSV procesado: ``src/pipelines/run_trm.py`` lo hace
    solo si pasan las validaciones de calidad.
    """
    csv_text = descargar_trm(config, obtener)
    if raw_dir is not None:
        guardar_crudo(csv_text, raw_dir, config)

    vigencias = parsear_trm_csv(csv_text, config)
    validar_vigencias_continuas(vigencias)
    verificar_actualidad(vigencias, hoy, config.max_dias_rezago)

    mensual = promedio_mensual(vigencias, config.ponderacion)
    df = limpiar_trm(mensual, config, hoy)
    logger.info(
        "TRM mensual (%s): %d meses, %s → %s",
        config.ponderacion, len(df), df["date"].min().date(), df["date"].max().date(),
    )
    return df
