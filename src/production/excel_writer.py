"""Escritura del Excel multi-hoja PIB_Potencial_Colombia.xlsx.

Produce un archivo con 4 hojas:

    Trimestral  — serie Cobb-Douglas desde 2005-Q1
    Mensual     — NAIRU*, NAICU*, indicadores mensuales de coyuntura
    Supuestos   — parámetros del modelo (lambda HP, alpha fallback, …)
    Metadatos   — fechas de descarga por fuente, versión del pipeline

El formato sigue el estilo del Boceto manual: encabezados en azul oscuro,
filas alternas en gris claro, números formateados por tipo de dato.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("nairu_pipeline.production.excel_writer")

OUTPUT_FILENAME = "PIB_Potencial_Colombia.xlsx"
PIPELINE_VERSION = "0.4.0"  # metodología alineada con v3 Módulo 2 (ver CHANGELOG [0.5.6])

# ── Paleta de colores (ARGB sin #) ────────────────────────────────────────────
COLOR_HEADER_BG   = "FF1F3864"   # azul oscuro EAFIT
COLOR_HEADER_FG   = "FFFFFFFF"   # blanco
COLOR_ROW_ALT     = "FFD9E1F2"   # azul gris claro
COLOR_ROW_NORMAL  = "FFFFFFFF"   # blanco
COLOR_SECTION_BG  = "FF2F5496"   # azul medio (sub-encabezados)
COLOR_SECTION_FG  = "FFFFFFFF"

# ── Definición de columnas de la hoja Trimestral ──────────────────────────────
#
# Alineación con v3 (2026-09-10, ver docs/integracion_v3.md): el motor de PIB
# potencial pasó de NIVELES (miles de personas, millones COP) a ÍNDICES base
# 100 en ``factors.BASE_QUARTER``, construidos sobre sumas móviles de 4
# trimestres. Por eso columnas como "L Obs./L Potencial" o "K Usado/K
# Potencial" (antes en miles de personas / millones COP) se reemplazan por
# ``idx_L``/``idx_L_star`` e ``idx_K``/``idx_K_star`` (índice, base=100). El
# PIB observado trimestral en niveles (``V_pib``) se conserva para contexto,
# separado del índice anualizado (``idx_pib``) que entra a la función de
# producción -- no son la misma magnitud y no deben leerse como comparables
# celda a celda.

TRIMESTRAL_COLS: list[dict[str, Any]] = [
    # (col_df, encabezado, ancho, formato_excel)
    dict(col="date",                         header="Fecha",              width=12, fmt="YYYY-MM-DD"),
    dict(col="year",                         header="Año",                width=7,  fmt="0"),
    dict(col="quarter",                      header="Trimestre",          width=11, fmt="0"),
    dict(col="V_pib",                        header="PIB Obs.\nTrim. (niveles)", width=16, fmt="#,##0.0"),
    dict(col="idx_pib",                      header="PIB Obs.\n(índice, base=100)", width=14, fmt="#,##0.0"),
    dict(col="PIB_tend_BHP",                 header="PIB Tend.\nBHP (índice)", width=14, fmt="#,##0.0"),
    dict(col="Brecha_BHP",                   header="Brecha BHP\n(%)",   width=12, fmt="0.00"),
    dict(col="K",                            header="Capital K\nDANE (niveles)", width=16, fmt="#,##0.0"),
    dict(col="icu",                          header="ICU Obs.\n(%)",     width=11, fmt="0.00"),
    dict(col="naicu",                        header="NAICU*\n(%)",       width=11, fmt="0.00"),
    dict(col="idx_K",                        header="K Obs.\n(índice, base=100)", width=14, fmt="#,##0.0"),
    dict(col="idx_K_star",                   header="K Potencial\n(índice, base=100)", width=14, fmt="#,##0.0"),
    dict(col="pet",                          header="PET\n(miles)",      width=12, fmt="#,##0.0"),
    dict(col="tgp",                          header="TGP\n(%)",          width=11, fmt="0.00"),
    dict(col="td",                           header="TD Obs.\n(%)",      width=11, fmt="0.00"),
    dict(col="tgp_star",                     header="TGP*\n(%)",         width=11, fmt="0.00"),
    dict(col="nairu",                        header="NAIRU*\n(%)",       width=11, fmt="0.00"),
    dict(col="jornada",                      header="Jornada legal\n(h/sem)", width=12, fmt="0"),
    dict(col="idx_L",                        header="L Obs.\n(índice, base=100)", width=14, fmt="#,##0.0"),
    dict(col="idx_L_star",                   header="L Potencial\n(índice, base=100)", width=14, fmt="#,##0.0"),
    dict(col="alpha",                        header="Alpha\n(cap/PIB, CBO)", width=11, fmt="0.000"),
    dict(col="BQ_ra",                        header="Remun.\nAsalar.",   width=14, fmt="#,##0.0"),
    dict(col="BS_ebe",                       header="Exc. Bruto\nExplot.", width=14, fmt="#,##0.0"),
    dict(col="A_obs",                        header="PTF Obs.\n(A)",     width=12, fmt="0.0000"),
    dict(col="A_pot",                        header="PTF* Tend.\nCBO (A_pot)", width=12, fmt="0.0000"),
    dict(col="PIB_pot",                      header="PIB Potencial\n(índice, base=100)", width=16, fmt="#,##0.0"),
    dict(col="Brecha_CD",                    header="Brecha CD\n(%)",    width=12, fmt="0.00"),
]

MENSUAL_COLS: list[dict[str, Any]] = [
    dict(col="date",                 header="Fecha",               width=12, fmt="YYYY-MM-DD"),
    dict(col="nairu_estimate",       header="NAIRU*\n(%)",         width=11, fmt="0.00"),
    dict(col="nairu_ci_lower_90",    header="NAIRU*\nIC90 inf",   width=13, fmt="0.00"),
    dict(col="nairu_ci_upper_90",    header="NAIRU*\nIC90 sup",   width=13, fmt="0.00"),
    dict(col="naicu_estimate",       header="NAICU*\n(%)",         width=11, fmt="0.00"),
    dict(col="unemployment_rate",    header="TD Obs.\n(%)",        width=11, fmt="0.00"),
    dict(col="tgp_rate",             header="TGP\n(%)",            width=11, fmt="0.00"),
    dict(col="capacity_utilization", header="UCI\n(%)",            width=11, fmt="0.00"),
    dict(col="ipc_yoy",              header="IPC interanual\n(%)", width=14, fmt="0.00"),
    dict(col="inflation_gap",        header="Brecha\nInflación",   width=13, fmt="0.00"),
]


# ── Helpers de formato ────────────────────────────────────────────────────────

def _try_openpyxl():
    """Importa openpyxl; lanza ImportError con mensaje claro si no está."""
    try:
        import openpyxl
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
        from openpyxl.utils import get_column_letter
        return openpyxl, Alignment, Border, Font, PatternFill, Side, get_column_letter
    except ImportError as exc:
        raise ImportError(
            "openpyxl es necesario para escribir el Excel. "
            "Instala con: pip install openpyxl"
        ) from exc


def _header_style(ws, row: int, n_cols: int, fill_color: str, font_color: str):
    """Aplica estilo de encabezado a una fila completa."""
    _, Alignment, Border, Font, PatternFill, Side, get_column_letter = _try_openpyxl()
    fill = PatternFill("solid", fgColor=fill_color)
    font = Font(bold=True, color=font_color, size=9)
    border_side = Side(style="thin", color="FF999999")
    border = Border(
        bottom=border_side, right=border_side,
    )
    align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for col_idx in range(1, n_cols + 1):
        cell = ws.cell(row=row, column=col_idx)
        cell.fill   = fill
        cell.font   = font
        cell.border = border
        cell.alignment = align


def _data_style(ws, row: int, n_cols: int, alternado: bool):
    """Aplica estilo de datos a una fila (alternando color)."""
    _, Alignment, _, _, PatternFill, Side, _ = _try_openpyxl()
    fgColor = COLOR_ROW_ALT if alternado else COLOR_ROW_NORMAL
    fill = PatternFill("solid", fgColor=fgColor)
    border_side = Side(style="hair", color="FFCCCCCC")
    from openpyxl.styles import Border
    border = Border(bottom=border_side, right=border_side)
    align_center = Alignment(horizontal="center", vertical="center")
    align_right  = Alignment(horizontal="right",  vertical="center")
    for col_idx in range(1, n_cols + 1):
        cell = ws.cell(row=row, column=col_idx)
        cell.fill   = fill
        cell.border = border
        cell.alignment = align_right if col_idx > 3 else align_center


def _write_sheet(
    ws,
    df: pd.DataFrame,
    col_defs: list[dict[str, Any]],
    title: str,
) -> None:
    """Escribe encabezado + datos en una hoja de openpyxl."""
    openpyxl, Alignment, _, Font, PatternFill, _, get_column_letter = _try_openpyxl()

    # Fila 1: título de la hoja
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(col_defs))
    title_cell = ws.cell(row=1, column=1, value=title)
    title_cell.font      = Font(bold=True, size=11, color=COLOR_HEADER_FG)
    title_cell.fill      = PatternFill("solid", fgColor=COLOR_SECTION_BG)
    title_cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 22

    # Fila 2: encabezados de columna
    for i, cdef in enumerate(col_defs, start=1):
        ws.cell(row=2, column=i, value=cdef["header"])
        ws.column_dimensions[get_column_letter(i)].width = cdef["width"]
    _header_style(ws, 2, len(col_defs), COLOR_HEADER_BG, COLOR_HEADER_FG)
    ws.row_dimensions[2].height = 30

    # Filas de datos
    for row_idx, (_, fila) in enumerate(df.iterrows(), start=3):
        alternado = (row_idx % 2 == 0)
        _data_style(ws, row_idx, len(col_defs), alternado)
        for col_idx, cdef in enumerate(col_defs, start=1):
            col = cdef["col"]
            val = fila.get(col, None) if hasattr(fila, "get") else (
                fila[col] if col in fila.index else None
            )
            # Convertir NaT/NaN a None para openpyxl
            if pd.isna(val) if not isinstance(val, str) else False:
                val = None
            # Convertir timestamps a date
            if isinstance(val, pd.Timestamp):
                val = val.date()
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            if val is not None and cdef.get("fmt"):
                cell.number_format = cdef["fmt"]

    # Congelar paneles: fila de encabezado + primera columna
    ws.freeze_panes = ws.cell(row=3, column=2)


# ── Hoja Supuestos ────────────────────────────────────────────────────────────

def _write_supuestos(ws, supuestos: dict[str, str]) -> None:
    openpyxl, Alignment, _, Font, PatternFill, Side, _ = _try_openpyxl()
    from openpyxl.styles import Border

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 55

    # Título
    ws.merge_cells("A1:B1")
    tc = ws.cell(row=1, column=1, value="Supuestos y parámetros del modelo")
    tc.font = Font(bold=True, size=11, color=COLOR_HEADER_FG)
    tc.fill = PatternFill("solid", fgColor=COLOR_SECTION_BG)
    tc.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 22

    # Encabezados
    for col, val in enumerate(["Parámetro", "Valor / descripción"], start=1):
        cell = ws.cell(row=2, column=col, value=val)
        cell.font = Font(bold=True, color=COLOR_HEADER_FG, size=9)
        cell.fill = PatternFill("solid", fgColor=COLOR_HEADER_BG)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[2].height = 18

    for i, (param, valor) in enumerate(supuestos.items(), start=3):
        alt = PatternFill("solid", fgColor=COLOR_ROW_ALT if i % 2 == 0 else COLOR_ROW_NORMAL)
        cell_a = ws.cell(row=i, column=1, value=param)
        cell_b = ws.cell(row=i, column=2, value=valor)
        cell_a.fill = cell_b.fill = alt
        cell_a.font = Font(bold=True, size=9)
        cell_b.font = Font(size=9)
        cell_a.alignment = cell_b.alignment = Alignment(vertical="center")

    ws.freeze_panes = "A3"


# ── Hoja Metadatos ────────────────────────────────────────────────────────────

def _write_metadatos(ws, metadatos: dict[str, str]) -> None:
    openpyxl, Alignment, _, Font, PatternFill, _, _ = _try_openpyxl()

    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 50

    ws.merge_cells("A1:B1")
    tc = ws.cell(row=1, column=1, value="Metadatos del pipeline")
    tc.font = Font(bold=True, size=11, color=COLOR_HEADER_FG)
    tc.fill = PatternFill("solid", fgColor=COLOR_SECTION_BG)
    tc.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 22

    for i, (clave, valor) in enumerate(metadatos.items(), start=2):
        alt = PatternFill("solid", fgColor=COLOR_ROW_ALT if i % 2 == 0 else COLOR_ROW_NORMAL)
        cell_a = ws.cell(row=i, column=1, value=clave)
        cell_b = ws.cell(row=i, column=2, value=str(valor))
        cell_a.fill = cell_b.fill = alt
        cell_a.font = Font(bold=True, size=9)
        cell_b.font = Font(size=9)


# ── Función principal ─────────────────────────────────────────────────────────

def write_pib_potencial_excel(
    df_quarterly: pd.DataFrame,
    df_monthly: pd.DataFrame,
    output_dir: Path,
    metadatos: dict[str, str] | None = None,
) -> Path:
    """Escribe el Excel PIB_Potencial_Colombia.xlsx con 4 hojas.

    Parameters
    ----------
    df_quarterly : pd.DataFrame
        Dataset trimestral con todas las columnas de ``QUARTERLY_OUTPUT_COLS``.
    df_monthly : pd.DataFrame
        Dataset mensual con NAIRU*, UCI, ipc_yoy, etc.
    output_dir : Path
        Directorio de salida (se crea si no existe).
    metadatos : dict, optional
        Información adicional para la hoja Metadatos (fechas de descarga, etc.).

    Returns
    -------
    Path
        Ruta al archivo Excel generado.
    """
    openpyxl, *_ = _try_openpyxl()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / OUTPUT_FILENAME

    wb = openpyxl.Workbook()

    # ── Hoja 1: Trimestral ────────────────────────────────────────────────
    ws_trim = wb.active
    ws_trim.title = "Trimestral"
    # Filtrar solo columnas presentes
    col_defs_trim = [c for c in TRIMESTRAL_COLS if c["col"] in df_quarterly.columns]
    _write_sheet(
        ws_trim,
        df_quarterly,
        col_defs_trim,
        "PIB Potencial Colombia — Función de Producción Cobb-Douglas (trimestral)",
    )

    # ── Hoja 2: Mensual ───────────────────────────────────────────────────
    ws_mens = wb.create_sheet("Mensual")
    col_defs_mens = [c for c in MENSUAL_COLS if c["col"] in df_monthly.columns]
    _write_sheet(
        ws_mens,
        df_monthly,
        col_defs_mens,
        "Indicadores mensuales de coyuntura — NAIRU*, NAICU*, UCI, Inflación",
    )

    # ── Hoja 3: Supuestos ─────────────────────────────────────────────────
    ws_sup = wb.create_sheet("Supuestos")
    supuestos = {
        "Metodología":
            "Alineada con legacy/pib_potencial_integrado_v3.py (Módulo 2, "
            "config. v2) — ver docs/integracion_v3.md",
        "Ancla de índices (BASE_QUARTER)":
            "Todos los índices (idx_*) valen 100 en este trimestre; ver "
            "src/production/factors.py para la justificación (huecos "
            "reales de datos: PIB DANE desde 2005-Q1 sin historia previa, "
            "GEIH sin dato jul-ago/2006)",
        "Lambda HP (datos trimestrales)":
            "1600  (Hodrick & Prescott, 1997 — estándar trimestral; una sola "
            "pasada, no Boosted-HP)",
        "TGP* (participación potencial)":
            "OLS en niveles: TGP = tendencia por tramos (picos del ciclo "
            "BBQ) + brecha_u + MA8(brecha_u) + brecha_icu + MA8(brecha_icu)",
        "Factor Trabajo (idx_L, idx_L_star)":
            "Horas trabajadas (Ocupados/Ocupados* × jornada legal, netas de "
            "vacaciones y festivos efectivos), suma móvil 4T, índice base 100",
        "Jornada legal":
            "Ley 2101 de 2021 — 48h hasta 2023-Q2, 47/46/44/42h desde "
            "sep-2023/2024/2025/2026",
        "Capital humano (idx_hc)":
            "PWT (human_capital), extrapolación OLS anclada tras el último "
            "año observado, interpolación intra-anual",
        "Factor Capital (idx_K, idx_K_star)":
            "Stock de capital productivo DANE (observado, anual → trimestral "
            "por interpolación PCHIP), × (ICU/100) u (NAICU*/100), suma móvil "
            "4T, índice base 100 — NO es Inventario Permanente (PIM)",
        "Alpha (participación del capital)":
            "EBE / (RA + EBE)  — estilo CBO, ventana 2016-Q1 → T (primer "
            "trimestre con datos de ingreso DANE)",
        "PTF observada (ptf / A_obs)":
            "idx_pib / (idx_K^alpha × idx_LH^(1−alpha))",
        "PTF tendencial estructural (ptf_star / A_pot)":
            "OLS de ln(PTF) sobre tendencia por tramos (rampas-meseta "
            "ancladas en picos BBQ) + brecha_u (contemp. y rezagada) + "
            "dummies de pandemia — PTF* = ajustado sin términos cíclicos",
        "PIB Potencial (principal, pib_pot / PIB_pot)":
            "PTF* × idx_K_star^alpha × idx_LH_star^(1−alpha)  [índice base 100]",
        "Brecha CD (%)":
            "(idx_pib / PIB_pot − 1) × 100",
        "Brecha BHP (%, referencia)":
            "(idx_pib / HP_trend(idx_pib) − 1) × 100  — sin pasar por la "
            "función de producción",
        "Fuente NAIRU*/NAICU*/ICU":
            "Kalman biestado — src/nairu/model_core.py "
            "(outputs/nairu/nairu_colombia.csv)",
        "Inicio de la serie de insumos":
            "2005-Q1  (primer trimestre con PIB DANE disponible); los "
            "índices solo están definidos desde BASE_QUARTER (ver arriba)",
    }
    _write_supuestos(ws_sup, supuestos)

    # ── Hoja 4: Metadatos ─────────────────────────────────────────────────
    ws_meta = wb.create_sheet("Metadatos")
    meta = {
        "Generado":          datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Versión pipeline":  PIPELINE_VERSION,
        "Script":            "python -m src.main --pib-potencial",
        "Repositorio":       "github.com/Tupxz/scraping-NAIRU",
    }
    if metadatos:
        meta.update(metadatos)
    _write_metadatos(ws_meta, meta)

    wb.save(out_path)
    logger.info("Excel guardado: %s", out_path)
    return out_path
