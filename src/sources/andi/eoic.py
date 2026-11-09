"""Scraping y actualización del ICU desde los informes EOIC de la ANDI.

1. **SCRAPING** — ``ANDIScraper`` lee la página de la ANDI, identifica los
   informes mensuales de la EOIC y los descarga a ``data/raw/andi/``.
2. **EXTRACCIÓN** — ``src/sources/andi/extraccion_icu.py``: texto por
   columnas, frase del dato del total de la industria y controles.
3. **PIPELINE** — ``run_andi_pipeline`` arma la serie final:

   - base = histórico de la ANDI (``data/inputs/icu_historical.csv``, dato
     dado, **nunca se sobrescribe**);
   - + meses nuevos ya agregados en corridas anteriores (filas de
     ``andi_capacidad_instalada.csv`` cuyo mes no está en el histórico);
   - + meses publicados en la página que no están en ninguno de los dos,
     en orden: se descarga el informe, se extrae el dato y se valida
     (mes del dato = mes del informe, rango 55–90, mes anterior presente y
     salto ≤ 8 pp).  Si algo falla se lanza error y no se escribe nada.

   Con ``respaldo_ia`` (``--andi-ia``, solo local) un mes cuyo texto no
   trae la frase se consulta a ``claude -p``; el número pasa por los mismos
   controles y queda marcado en ``source``.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import (
    ANDI_CONFIG,
    ANDI_PROCESSED_COLUMNS,
    CAPACITY_UTILIZATION_MAX,
    CAPACITY_UTILIZATION_MAX_CHANGE,
    CAPACITY_UTILIZATION_MIN,
    INPUTS_DIR,
    PROCESSED_DIR,
    RAW_ANDI_DIR,
)
from src.sources.andi.extraccion_icu import (
    ICUControlError,
    ICUExtraccionError,
    extraer_icu_de_texto,
    mes_en_portada,
    mes_en_texto,
    normalizar,
    texto_por_columnas,
    validar_icu_nuevo,
)

logger = logging.getLogger("nairu_pipeline.andi")

# Respaldo para meses sin frase: (texto, mes) -> (valor, etiqueta para `source`).
RespaldoICU = Callable[[str, pd.Period], tuple[float, str]]

# ═══════════════════════════════════════════════════════════════════════
# Constantes locales (derivadas de AndiConfig)
# ═══════════════════════════════════════════════════════════════════════

_EOIC_REQUIRED = re.compile(ANDI_CONFIG.eoic_required_pattern, re.IGNORECASE)
_EOIC_BROAD = re.compile(ANDI_CONFIG.eoic_broad_pattern, re.IGNORECASE)
_EXCLUDE = re.compile(ANDI_CONFIG.eoic_exclude_pattern, re.IGNORECASE)
_PERCENT_RE = re.compile(ANDI_CONFIG.percent_pattern)

_MONTH_MAP: dict[str, int] = ANDI_CONFIG.month_map

# Primer mes del histórico ICU de la ANDI (data/inputs/icu_historical.csv).
ICU_HISTORICAL_START: str = "2001-01-01"


# ═══════════════════════════════════════════════════════════════════════
# Capa 1 — Scraping: ANDIScraper
# ═══════════════════════════════════════════════════════════════════════


class ANDIScraper:
    """Descarga PDFs de la EOIC desde la página de la ANDI.

    Parameters
    ----------
    data_dir : Path | None
        Directorio donde guardar los PDFs descargados.
        Por defecto ``RAW_ANDI_DIR``.
    """

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir or RAW_ANDI_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        retry = Retry(
            total=ANDI_CONFIG.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.headers.update(ANDI_CONFIG.http_headers)

    # ── Obtención de HTML ─────────────────────────────────────────

    def get_page_content(self, url: str) -> str | None:
        """Descarga el HTML de *url* con reintentos.

        Returns
        -------
        str | None
            HTML como texto, o ``None`` si falla.
        """
        for attempt in range(1, ANDI_CONFIG.max_retries + 1):
            try:
                resp = self.session.get(
                    url, timeout=ANDI_CONFIG.timeout,
                )
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as exc:
                logger.warning(
                    "Intento %d/%d fallido para %s: %s",
                    attempt, ANDI_CONFIG.max_retries, url, exc,
                )
                if attempt < ANDI_CONFIG.max_retries:
                    time.sleep(2 * attempt)
        return None

    # ── Identificación de enlaces EOIC ────────────────────────────

    @staticmethod
    def _is_eoic_report(url: str, text: str) -> bool:
        """Determina si un enlace corresponde a un informe EOIC."""
        combined = f"{url} {text}".lower()
        if _EXCLUDE.search(combined):
            return False
        return bool(
            _EOIC_REQUIRED.search(combined) or _EOIC_BROAD.search(combined)
        )

    @staticmethod
    def _extract_date(url: str, text: str) -> str | None:
        """Extrae fecha YYYY-MM de la URL o texto del enlace.

        Busca co-ocurrencias ``mes + año`` próximas en el texto combinado
        para evitar falsos positivos cuando el texto menciona varios meses
        (ej. comparaciones interanuales).

        Returns
        -------
        str | None
            Fecha en formato ``"YYYY-MM"`` o ``None``.
        """
        # La URL se decodifica antes de buscar: en "enero%202026" el patrón
        # de año tomaba "2020" ("%20" + "20…") y los informes de 2026 se
        # fechaban en 2020 (por eso el ICU no se actualizaba).
        combined = f"{unquote(url)} {text}".lower()

        # Estrategia 1: buscar patrones "mes (de) año" o "año ... mes"
        # cercanos entre sí.
        best: tuple[int, str] | None = None  # (pos, "YYYY-MM")
        for name, num in _MONTH_MAP.items():
            for m in re.finditer(re.escape(name), combined):
                # Buscar un año 20xx en un radio de 30 chars alrededor.
                start = max(0, m.start() - 30)
                end = min(len(combined), m.end() + 30)
                region = combined[start:end]
                year_m = re.search(r"(?<!\d)20[0-3]\d(?!\d)", region)
                if year_m:
                    pos = m.start()
                    candidate = f"{year_m.group()}-{num:02d}"
                    if best is None or pos < best[0]:
                        best = (pos, candidate)

        if best is not None:
            return best[1]

        # Estrategia 2: mes numérico en la URL.
        m = re.search(r"(\d{4})[_/-](\d{2})", combined)
        if m and 1 <= int(m.group(2)) <= 12:
            return f"{m.group(1)}-{m.group(2)}"

        return None

    def find_eoic_links(
        self, html: str, *, strict: bool = True,
    ) -> list[dict[str, str | None]]:
        """Extrae enlaces a PDFs de la EOIC desde el HTML.

        Parameters
        ----------
        html : str
            Contenido HTML de la página.
        strict : bool
            Si ``True`` aplica filtros estrictos primero; si no encuentra
            nada, relaja a filtro amplio.

        Returns
        -------
        list[dict]
            Lista de dicts con claves ``url``, ``text``, ``date``, ``href``.
        """
        soup = BeautifulSoup(html, "lxml")
        results: list[dict[str, str | None]] = []
        seen_urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href: str = a_tag["href"]
            text: str = a_tag.get_text(strip=True)

            # Construir URL absoluta.
            if href.startswith("/"):
                full_url = ANDI_CONFIG.base_url + href
            elif href.startswith("http"):
                full_url = href
            else:
                continue

            if not self._is_eoic_report(full_url, text):
                continue

            norm_url = full_url.split("?")[0].lower()
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)

            results.append({
                "url": full_url,
                "text": text,
                "date": self._extract_date(full_url, text),
                "href": href,
            })

        # Fallback amplio si strict no encontró nada.
        if strict and not results:
            logger.info("Búsqueda estricta vacía; intentando filtro amplio.")
            return self.find_eoic_links(html, strict=False)

        return sorted(results, key=lambda d: d.get("date") or "")

    def get_all_eoic_pdfs(self) -> list[dict[str, str | None]]:
        """Obtiene todos los enlaces EOIC (para backfill).

        Returns
        -------
        list[dict]
            Enlaces ordenados por fecha (más antiguo primero).
        """
        html = self.get_page_content(ANDI_CONFIG.eoic_page_url)
        if not html:
            logger.error("No se pudo obtener la página de la ANDI.")
            return []
        return self.find_eoic_links(html)

    def get_new_eoic_pdfs(
        self, existing_dates: set[str]
    ) -> list[dict[str, str | None]]:
        """Obtiene los PDFs EOIC cuya fecha no está en *existing_dates*.

        Estrategia eficiente de 2 pasos:
        1. Descarga la página UNA sola vez y obtiene todos los enlaces.
        2. Devuelve solo los que no están en ``existing_dates``.

        Esto detecta meses intermedios perdidos (si el pipeline estuvo
        inactivo varias semanas) sin necesitar llamadas extra a la red.
        La carga de página ocurre una sola vez — igual que antes.

        Parameters
        ----------
        existing_dates : set[str]
            Fechas ya presentes (formato ``YYYY-MM-01``).

        Returns
        -------
        list[dict]
            PDFs nuevos, ordenados por fecha (más antiguo primero).
        """
        pdfs = self.get_all_eoic_pdfs()   # 1 sola petición HTTP

        # Normalizar existing_dates a YYYY-MM (para comparar con las fechas
        # de los PDFs que vienen en formato YYYY-MM desde la web de la ANDI).
        existing_ym = {d[:7] for d in existing_dates if d}

        new = [p for p in pdfs if (p.get("date") or "")[:7] not in existing_ym]
        if not new:
            logger.info("Incremental: ningún PDF nuevo encontrado.")
        else:
            logger.info(
                "Incremental: %d PDF(s) nuevo(s): %s",
                len(new),
                ", ".join(p.get("date", "?") for p in new),
            )
        return new

    def download_pdf(self, url: str) -> Path | None:
        """Descarga un PDF a ``self.data_dir``.

        Si el archivo ya existe, no lo vuelve a descargar.

        Returns
        -------
        Path | None
            Ruta al archivo descargado, o ``None`` si falla.
        """
        filename = url.split("/")[-1].split("?")[0]
        if not filename.endswith(".pdf"):
            filename += ".pdf"
        local_path = self.data_dir / filename

        if local_path.exists():
            logger.debug("Ya existe: %s", local_path.name)
            return local_path

        try:
            resp = self.session.get(url, timeout=ANDI_CONFIG.timeout)
            resp.raise_for_status()
            local_path.write_bytes(resp.content)
            logger.info("Descargado: %s (%.1f KB)", filename, len(resp.content) / 1024)
            return local_path
        except requests.RequestException as exc:
            logger.error("Error descargando %s: %s", url, exc)
            return None


# ── CSV helpers ───────────────────────────────────────────────────────


def _load_existing_dates(csv_path: Path) -> set[str]:
    """Devuelve el set de fechas ya presentes en el CSV, en formato YYYY-MM-01."""
    if not csv_path.exists():
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=["date"])
        # Normalizar a YYYY-MM-01 para comparaciones consistentes.
        return set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-01"))
    except Exception:
        return set()


def _load_icu_historical(
    historical_path: Path | None = None,
) -> list[dict]:
    """Carga la serie ICU histórica semilla (data/inputs/icu_historical.csv).

    Esta serie cubre 2001–2025 y sirve como base histórica para el pipeline
    ANDI.  El pipeline solo necesita añadir observaciones **posteriores** a
    la última fecha de este archivo.

    Returns
    -------
    list[dict]
        Registros en el esquema estándar de ``andi_capacidad_instalada.csv``.
        Lista vacía si el archivo no existe.
    """
    path = historical_path or (INPUTS_DIR / "icu_historical.csv")
    if not path.exists():
        logger.debug("[ANDI] icu_historical.csv no encontrado en %s — se omite semilla.", path)
        return []
    try:
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # El histórico de la ANDI arranca en 2001-01 (2001–2003 vienen del
        # Data_NAIRU.xlsx del commit a30fb4b; v7 desestacionaliza con STL
        # desde 2001).  Cualquier fila anterior es un error de la semilla.
        pre_mask = df["date"] < ICU_HISTORICAL_START
        if pre_mask.any():
            logger.warning(
                "[ANDI] icu_historical.csv: descartando %d filas anteriores a "
                "%s (fechas incorrectas en la semilla).",
                pre_mask.sum(), ICU_HISTORICAL_START,
            )
            df = df[~pre_mask].reset_index(drop=True)

        records = df[ANDI_PROCESSED_COLUMNS].to_dict("records")
        logger.info(
            "[ANDI] Semilla histórica ICU cargada: %d obs (%s → %s)",
            len(records), df["date"].iloc[0], df["date"].iloc[-1],
        )
        return records
    except Exception as exc:
        logger.warning("[ANDI] Error cargando icu_historical.csv: %s", exc)
        return []


def _build_record(
    date_str: str,
    value: float,
    source_url: str,
) -> dict[str, Any]:
    """Construye un registro con el esquema estándar del proyecto."""
    # Normalizar a YYYY-MM-01 si llega en formato YYYY-MM
    if len(date_str) == 7:
        date_str = f"{date_str}-01"
    parts = date_str.split("-")
    return {
        "date": date_str,
        "year": int(parts[0]),
        "month": int(parts[1]),
        "capacity_utilization": value,
        "source": source_url,
        "download_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def _save_dataframe(records: list[dict[str, Any]], csv_path: Path) -> pd.DataFrame:
    """Construye, ordena, deduplica y guarda el DataFrame procesado."""
    if not records:
        return pd.DataFrame(columns=ANDI_PROCESSED_COLUMNS)
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset="date", keep="last")
    df = df.reset_index(drop=True)
    df = df[ANDI_PROCESSED_COLUMNS]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Guardado: %s (%d filas)", csv_path.name, len(df))
    return df


# ── Reporte ───────────────────────────────────────────────────────────


def generate_report(
    records: list[dict[str, Any]],
    stats: dict[str, Any],
    report_path: Path,
) -> Path:
    """Genera un reporte de texto con el resumen del pipeline.

    Parameters
    ----------
    records : list[dict]
        Registros procesados exitosamente.
    stats : dict
        Estadísticas del pipeline (processed, ok, failed, skipped).
    report_path : Path
        Ruta de salida del reporte.

    Returns
    -------
    Path
        Ruta al reporte generado.
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "=" * 60,
        "REPORTE ANDI — EOIC Capacidad Instalada",
        f"Generado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
        "── Resumen ──",
        f"  PDFs procesados:           {stats.get('processed', 0)}",
        f"  Extracciones exitosas:     {stats.get('ok', 0)}",
        f"  Extracciones fallidas:     {stats.get('failed', 0)}",
        f"  Omitidos (ya existían):    {stats.get('skipped', 0)}",
        "",
    ]

    # Meses faltantes.
    if records:
        dates = sorted(r["date"] for r in records)
        all_months = pd.date_range(
            start=dates[0], end=dates[-1], freq="MS",
        )
        existing = set(dates)
        missing = [
            d.strftime("%Y-%m-%d")
            for d in all_months if d.strftime("%Y-%m-%d") not in existing
        ]
        if missing:
            lines.append("── Meses faltantes ──")
            for m in missing:
                lines.append(f"  {m}")
            lines.append("")

    # Valores sospechosos.
    suspicious: list[str] = []
    sorted_records = sorted(records, key=lambda r: r["date"])
    for i, rec in enumerate(sorted_records):
        val = rec["capacity_utilization"]
        if val < CAPACITY_UTILIZATION_MIN or val > CAPACITY_UTILIZATION_MAX:
            suspicious.append(
                f"  {rec['date']}  val={val:.1f}%  "
                f"FUERA DE RANGO [{CAPACITY_UTILIZATION_MIN}, "
                f"{CAPACITY_UTILIZATION_MAX}]  src={rec['source']}"
            )
        if i > 0:
            prev_val = sorted_records[i - 1]["capacity_utilization"]
            change = abs(val - prev_val)
            if change > CAPACITY_UTILIZATION_MAX_CHANGE:
                suspicious.append(
                    f"  {rec['date']}  val={val:.1f}%  "
                    f"Δ={change:.1f}pp (prev={prev_val:.1f}%)  "
                    f"src={rec['source']}"
                )

    if suspicious:
        lines.append("── Valores sospechosos ──")
        lines.extend(suspicious)
        lines.append("")

    lines.append("═" * 60)

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Reporte generado: %s", report_path)
    return report_path




# ═══════════════════════════════════════════════════════════════════════
# Capa 3 — Pipeline
# ═══════════════════════════════════════════════════════════════════════


def es_informe_eoic_pdf(url_o_nombre: str) -> bool:
    """Informe mensual EOIC en PDF (no infografías ni 'Prensa cuadros' .xls)."""
    nombre = normalizar(unquote(url_o_nombre.split("/")[-1].split("?")[0]))
    return (
        nombre.endswith(".pdf")
        and "eoic" in nombre
        and "infograf" not in nombre
        and "prensa" not in nombre
        and ".xls" not in nombre
    )


def mes_de_enlace(enlace: dict[str, Any]) -> pd.Period | None:
    """Mes del informe según la página (``date`` del enlace) o su URL."""
    fecha = enlace.get("date")
    if fecha:
        return pd.Period(str(fecha)[:7], freq="M")
    return mes_en_texto(unquote(str(enlace.get("url", ""))))


def _registros_a_serie(registros: list[dict[str, Any]]) -> pd.Series:
    if not registros:
        return pd.Series(dtype=float)
    df = pd.DataFrame(registros)
    idx = pd.to_datetime(df["date"]).dt.to_period("M")
    return pd.Series(df["capacity_utilization"].astype(float).to_numpy(), index=idx).sort_index()


def cargar_base_icu(
    csv_path: Path,
    historico_path: Path | None = None,
    *,
    incluir_previos: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(histórico, meses nuevos ya agregados) en el esquema ANDI.

    Los "previos" son filas del CSV procesado cuyo mes no está en el
    histórico (se agregaron en corridas anteriores con los mismos controles).
    """
    historico = _load_icu_historical(historico_path)
    if not historico:
        raise RuntimeError(
            "No se pudo cargar data/inputs/icu_historical.csv: sin histórico no se "
            "actualiza el ICU (el histórico de la ANDI es la base de la serie)."
        )
    if not incluir_previos or not csv_path.exists():
        return historico, []
    meses_hist = {r["date"][:7] for r in historico}
    proc = pd.read_csv(csv_path)
    proc["date"] = pd.to_datetime(proc["date"]).dt.strftime("%Y-%m-%d")
    previos = [r for r in proc[ANDI_PROCESSED_COLUMNS].to_dict("records")
               if r["date"][:7] not in meses_hist]
    return historico, previos


def actualizar_icu(
    informes: list[dict[str, Any]],
    serie: pd.Series,
    *,
    respaldo_ia: RespaldoICU | None = None,
) -> list[dict[str, Any]]:
    """Agrega, en orden, los meses de ``informes`` que no están en ``serie``.

    Parameters
    ----------
    informes : list[dict]
        Cada uno con ``mes`` (``pd.Period`` o ``None`` → se lee de la
        portada), ``origen`` (URL o ``local://nombre``) y ``obtener``
        (callable sin argumentos que devuelve la ruta local del PDF).
    serie : pd.Series
        ICU existente (índice ``pd.Period`` mensual).  No se modifica.
    respaldo_ia : callable, opcional
        Solo se usa si el texto del informe no trae la frase del dato
        (``ICUExtraccionError``).  Su número pasa por los mismos controles.

    Returns
    -------
    list[dict]
        Registros nuevos en el esquema de ``andi_capacidad_instalada.csv``.

    Raises
    ------
    ICUExtraccionError, ICUControlError, RuntimeError
        Ante la primera falla; el llamador no debe escribir nada.
    """
    serie = serie.copy()
    pendientes: dict[pd.Period, dict[str, Any]] = {}
    for inf in informes:
        mes, ruta, paginas = inf.get("mes"), None, None
        if mes is None:
            ruta = inf["obtener"]()
            paginas = texto_por_columnas(ruta)
            mes = mes_en_portada(paginas)
            if mes is None:
                logger.warning("[ANDI] Sin mes identificable, se omite: %s", inf["origen"])
                continue
        if mes in serie.index:
            continue                       # histórico o ya agregado: nunca se sobrescribe
        if mes in pendientes:
            logger.info("[ANDI] %s ya tiene informe (%s); se omite %s",
                        mes, pendientes[mes]["origen"], inf["origen"])
            continue
        pendientes[mes] = {**inf, "mes": mes, "ruta": ruta, "paginas": paginas}

    nuevos: list[dict[str, Any]] = []
    for mes in sorted(pendientes):
        inf = pendientes[mes]
        ruta = inf["ruta"] or inf["obtener"]()
        paginas = inf["paginas"] or texto_por_columnas(ruta)
        texto = "\n".join(paginas)
        portada = mes_en_portada(paginas)
        if portada is not None and portada != mes:
            logger.warning("[ANDI] La portada de %s dice %s (se usa el mes del enlace).",
                           inf["origen"], portada)
        try:
            dato = extraer_icu_de_texto(texto, mes)
            valor, fuente = dato.valor, inf["origen"]
            logger.info("[ANDI] %s = %.1f%% (regla %s): %s", mes, valor, dato.regla, dato.frase[:90])
        except ICUExtraccionError:
            if respaldo_ia is None:
                raise
            logger.warning("[ANDI] %s: el parser no encontró el dato; se consulta el respaldo IA.", mes)
            valor, etiqueta = respaldo_ia(texto, mes)
            fuente = f"{etiqueta}|{inf['origen']}"
        validar_icu_nuevo(mes, valor, serie)
        serie.loc[mes] = valor
        nuevos.append(_build_record(str(mes), valor, fuente))
    return nuevos


def _combinar(registros: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(registros)
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].duplicated().any():
        raise ICUControlError(f"Meses duplicados en la serie ICU: {df.loc[df['date'].duplicated(), 'date'].tolist()}")
    return df.sort_values("date").reset_index(drop=True)[ANDI_PROCESSED_COLUMNS]


def _descargar(scraper: ANDIScraper, url: str) -> Path:
    ruta = scraper.download_pdf(url)
    if ruta is None:
        raise RuntimeError(f"No se pudo descargar el informe EOIC: {url}")
    with open(ruta, "rb") as fh:
        if fh.read(5) != b"%PDF-":
            raise RuntimeError(f"El archivo descargado no es un PDF: {ruta.name}")
    return ruta


def run_andi_pipeline(
    *,
    backfill: bool = False,
    respaldo_ia: RespaldoICU | None = None,
    scraper: ANDIScraper | None = None,
    csv_path: Path | None = None,
    historico_path: Path | None = None,
) -> pd.DataFrame:
    """Serie ICU final (histórico + meses nuevos de la web).  **No escribe.**

    ``src/pipelines/run_andi.py`` valida el resultado y recién entonces lo
    guarda con :func:`guardar_icu`.

    Parameters
    ----------
    backfill : bool
        Si ``True``, vuelve a extraer también los meses nuevos que ya
        estaban en el CSV procesado (el histórico nunca se toca).
    respaldo_ia : callable, opcional
        Ver :func:`actualizar_icu` (``--andi-ia``).
    """
    csv_path = csv_path or (PROCESSED_DIR / ANDI_CONFIG.processed_filename)
    scraper = scraper or ANDIScraper()
    historico, previos = cargar_base_icu(csv_path, historico_path, incluir_previos=not backfill)
    serie = _registros_a_serie(historico + previos)

    enlaces = [e for e in scraper.get_all_eoic_pdfs() if es_informe_eoic_pdf(str(e.get("url", "")))]
    if not enlaces:
        raise RuntimeError("No se encontraron informes EOIC (PDF) en la página de la ANDI.")
    informes = [
        {"mes": mes_de_enlace(e), "origen": e["url"],
         "obtener": (lambda u=e["url"]: _descargar(scraper, u))}
        for e in enlaces
    ]
    logger.info(
        "[ANDI] %d informes en la página; serie actual %s → %s (%d meses)",
        len(informes), serie.index.min(), serie.index.max(), len(serie),
    )
    nuevos = actualizar_icu(informes, serie, respaldo_ia=respaldo_ia)
    if nuevos:
        logger.info("[ANDI] Meses nuevos: %s", [r["date"][:7] for r in nuevos])
    else:
        logger.info("[ANDI] Sin meses nuevos (último: %s).", serie.index.max())
    return _combinar(historico + previos + nuevos)


def reprocess_local_pdfs(
    *,
    respaldo_ia: RespaldoICU | None = None,
    pdf_dir: Path | None = None,
    csv_path: Path | None = None,
    historico_path: Path | None = None,
    escribir: bool = True,
) -> pd.DataFrame:
    """Igual que el pipeline, pero con los PDF de ``data/raw/andi/`` (sin red).

    El mes de cada archivo sale de su nombre (o de la portada si el nombre
    no trae año).  Valida y escribe solo si todo pasa.
    """
    from src.quality_checks import run_andi_checks

    pdf_dir = pdf_dir or RAW_ANDI_DIR
    csv_path = csv_path or (PROCESSED_DIR / ANDI_CONFIG.processed_filename)
    historico, previos = cargar_base_icu(csv_path, historico_path)
    serie = _registros_a_serie(historico + previos)
    informes = [
        {"mes": mes_en_texto(unquote(p.name)), "origen": f"local://{unquote(p.name)}",
         "obtener": (lambda p=p: p)}
        for p in sorted(pdf_dir.glob("*.pdf"))
        if es_informe_eoic_pdf(p.name) and p.read_bytes()[:5] == b"%PDF-"
    ]
    nuevos = actualizar_icu(informes, serie, respaldo_ia=respaldo_ia)
    df = _combinar(historico + previos + nuevos)
    run_andi_checks(df)
    if escribir:
        guardar_icu(df, csv_path)
    return df


def guardar_icu(df: pd.DataFrame, csv_path: Path | None = None) -> Path:
    """Escribe ``andi_capacidad_instalada.csv`` (fechas ``YYYY-MM-DD``)."""
    csv_path = csv_path or (PROCESSED_DIR / ANDI_CONFIG.processed_filename)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, date_format="%Y-%m-%d")
    logger.info("[ANDI] Guardado: %s (%d filas, %s → %s)", csv_path.name, len(df),
                pd.Timestamp(df["date"].min()).date(), pd.Timestamp(df["date"].max()).date())
    return csv_path


# ═══════════════════════════════════════════════════════════════════════
# Validación del parser contra el histórico
# ═══════════════════════════════════════════════════════════════════════


def validar_parser_contra_historico(
    pdf_dir: Path | None = None,
    historico_path: Path | None = None,
) -> pd.DataFrame:
    """Corre el extractor sobre los PDF locales y compara con el histórico.

    Un informe por contenido (los duplicados por nombre codificado se
    descartan por md5).  ``estado``: ``ok``, ``sin_dato``
    (``ICUExtraccionError``), ``control`` (``ICUControlError``) o ``no_pdf``.
    ``error`` = valor del informe − histórico (solo meses del histórico).
    """
    import hashlib

    pdf_dir = pdf_dir or RAW_ANDI_DIR
    hist = _registros_a_serie(_load_icu_historical(historico_path))
    vistos: set[str] = set()
    filas: list[dict[str, Any]] = []
    for p in sorted(pdf_dir.glob("*.pdf")):
        if not es_informe_eoic_pdf(p.name):
            continue
        contenido = p.read_bytes()
        md5 = hashlib.md5(contenido).hexdigest()
        if md5 in vistos:
            continue
        vistos.add(md5)
        fila: dict[str, Any] = {"archivo": unquote(p.name)}
        if contenido[:5] != b"%PDF-":
            filas.append({**fila, "estado": "no_pdf", "motivo": "el archivo no es un PDF"})
            continue
        paginas = texto_por_columnas(p)
        mes = mes_en_texto(unquote(p.name)) or mes_en_portada(paginas)
        fila["mes"] = str(mes)
        fila["historico"] = float(hist.loc[mes]) if mes in hist.index else None
        try:
            dato = extraer_icu_de_texto("\n".join(paginas), mes)
            fila.update(valor=dato.valor, regla=dato.regla, estado="ok")
            if fila["historico"] is not None:
                fila["error"] = round(dato.valor - fila["historico"], 4)
        except ICUExtraccionError as exc:
            fila.update(estado="sin_dato", motivo=str(exc))
        except ICUControlError as exc:
            fila.update(estado="control", motivo=str(exc))
        filas.append(fila)
    cols = ["archivo", "mes", "valor", "regla", "historico", "error", "estado", "motivo"]
    return pd.DataFrame(filas).reindex(columns=cols)


def resumen_validacion(df: pd.DataFrame) -> dict[str, Any]:
    """Error medio y máximo (absolutos) y conteos por estado."""
    ok = df[(df["estado"] == "ok") & df["error"].notna()]
    e = ok["error"].abs()
    return {
        "informes": int(len(df)),
        "por_estado": df["estado"].value_counts().to_dict(),
        "comparados": int(len(ok)),
        "error_medio_abs": float(e.mean()) if len(e) else float("nan"),
        "error_mediano_abs": float(e.median()) if len(e) else float("nan"),
        "error_max_abs": float(e.max()) if len(e) else float("nan"),
        "mes_error_max": ok.loc[e.idxmax(), "mes"] if len(e) else None,
        "dentro_redondeo": int((e <= 0.05 + 1e-9).sum()),
    }
