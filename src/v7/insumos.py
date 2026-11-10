"""Arma la carpeta de insumos que lee el motor v7 (``src/v7/motor.py``).

El motor lee todo relativo a una raíz con esta estructura (la del script de
referencia del compañero):

    <raiz>/Data_NAIRU.xlsx
    <raiz>/trm_cop_usd_promedio_mensual.csv
    <raiz>/festivos_efectivos_colombia_2001_2026.xlsx
    <raiz>/Filtros_PIB_Colombia_CF_dos_colas.xlsx      (opcional: solo el paso 3)
    <raiz>/datos/insumos_pib_trimestral.csv            date, V_pib, X_vapg, BE_inv, BQ_ra, BR_tms, BS_ebe, BT_im
    <raiz>/datos/insumos_pib_mensual.csv               date, pet, fl, ocup (personas)
    <raiz>/datos/insumos_pib_hc_anual.csv              year, hc
    <raiz>/datos/dane_stock_capital_productivo.csv     year, K_prod_mmp
    <raiz>/datos/eme_expectativas_inflacion_total_mensual_promedio.csv
    <raiz>/datos/emmet_salarios_v7.csv
    <raiz>/datos/geih_subempleo_bruto_mensual.csv
    <raiz>/datos/geih_tgp_to_td_desest_mensual.csv
    <raiz>/datos/banrep_vacantes_prensa_mensual.csv

Dos modos:

- ``"referencia"``: copia los insumos exactos con que se generó la línea base
  (``legacy/v7/datos/``, ``data/inputs/archive/Data_NAIRU_2026-05-11_icu2001.xlsx``).
  Sirve para verificar que el motor reproduce ``tests/fixtures/baseline_v7/``.
- ``"repo"``: construye cada insumo desde ``data/processed/`` y ``data/inputs/``.
  Cuando una serie todavía no la produce el scraping, usa la de referencia y lo
  deja registrado. El dict que devuelve ``preparar_raiz`` dice de dónde salió
  cada archivo; ``run_v7`` lo guarda en ``fuentes_insumos.json``.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from src.config import INPUTS_DIR, OUTPUTS_DIR, PROCESSED_DIR, PROJECT_ROOT

logger = logging.getLogger("nairu_pipeline.v7.insumos")

LEGACY_V7 = PROJECT_ROOT / "legacy" / "v7"
REF_DATOS = LEGACY_V7 / "datos"
REF_DATA_NAIRU = INPUTS_DIR / "archive" / "Data_NAIRU_2026-05-11_icu2001.xlsx"
REF_TRM = LEGACY_V7 / "trm_cop_usd_promedio_mensual.csv"
FESTIVOS = INPUTS_DIR / "festivos_efectivos_colombia_2001_2026.xlsx"
CAPITAL = INPUTS_DIR / "alt_capital" / "dane_stock_capital_productivo.csv"
FILTROS = OUTPUTS_DIR / "viog_colombia" / "Filtros_PIB_Colombia_CF_dos_colas.xlsx"

# Series mensuales del bloque laboral que el scraping irá produciendo
# (rama scrapers-v7). Mientras no existan en data/processed/, se usa la referencia.
MENSUALES_LAB = {
    "eme_expectativas_inflacion_total_mensual_promedio.csv": ["date", "doce_meses"],
    "emmet_salarios_v7.csv": ["date", "var_anual_salario_nominal"],
    "geih_subempleo_bruto_mensual.csv": ["date", "tasa_subocupacion"],
    "geih_tgp_to_td_desest_mensual.csv": ["date", "tgp_desest"],
    "banrep_vacantes_prensa_mensual.csv": ["date"],
}

MODOS = ("repo", "referencia")


# --------------------------------------------------------------------------- #
#  Utilidades                                                                  #
# --------------------------------------------------------------------------- #
def _copiar(src: Path, dst: Path) -> str:
    if not src.exists():
        raise FileNotFoundError(f"Falta el insumo {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    try:
        return str(src.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(src)


def _inicio_trimestre_a_cierre(fechas: pd.Series) -> pd.Series:
    """El repo fecha trimestres por el mes de inicio; v7 por el mes de cierre."""
    return fechas.dt.to_period("Q").dt.end_time.dt.to_period("M").dt.to_timestamp()


def data_nairu_cumple_v7(path: Path) -> tuple[bool, str]:
    """Data_NAIRU.xlsx sirve para v7 si (i) el ICU arranca en 2001-01 sin huecos
    (el STL de v7 usa toda su historia) y (ii) ``Unemp_Desest`` es la TD
    desestacionalizada PUBLICADA por el DANE (un decimal), decisión del usuario."""
    if not path.exists():
        return False, "no existe"
    d = pd.read_excel(path)
    d["f"] = pd.to_datetime(dict(year=d["Year"], month=d["Month"], day=1))
    icu = d.dropna(subset=["ICU"])
    if icu.empty or icu["f"].min() > pd.Timestamp("2001-01-01"):
        return False, f"ICU arranca en {icu['f'].min().date() if not icu.empty else '—'} (v7 necesita 2001-01)"
    u = d["Unemp_Desest"].dropna()
    if not np.allclose(u, u.round(1), atol=1e-9):
        return False, "Unemp_Desest no es la TD publicada (no tiene un decimal)"
    return True, "ok"


# --------------------------------------------------------------------------- #
#  Constructores desde data/processed (modo repo)                             #
# --------------------------------------------------------------------------- #
def _trimestral_repo(ref: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, str]]:
    fuentes: Dict[str, str] = {}
    gdp = pd.read_csv(PROCESSED_DIR / "dane_gdp_colombia.csv", parse_dates=["date"])
    gdp["date"] = _inicio_trimestre_a_cierre(gdp["date"])
    out = gdp[["date"]].copy()
    out["V_pib"] = gdp["gdp_observed"].to_numpy()

    # 2004: el DANE base 2015 arranca en 2005T1; se encadenan las tasas de la
    # referencia (empalme con la base 2005) sobre el primer dato del repo.
    ref_v = ref.set_index("date")["V_pib"]
    primero = out["date"].min()
    previos = ref_v[ref_v.index < primero].dropna()
    if len(previos) and primero in ref_v.index:
        escala = out.loc[out["date"] == primero, "V_pib"].iloc[0] / ref_v[primero]
        out = pd.concat([pd.DataFrame({"date": previos.index, "V_pib": previos.to_numpy() * escala}), out])
        fuentes["V_pib"] = ("data/processed/dane_gdp_colombia.csv; 2004 encadenado con las "
                            "tasas de la referencia (empalme base 2005)")
    else:
        fuentes["V_pib"] = "data/processed/dane_gdp_colombia.csv"

    inc = pd.read_csv(PROCESSED_DIR / "dane_gdp_income_colombia.csv", parse_dates=["date"])
    inc["date"] = _inicio_trimestre_a_cierre(inc["date"])
    inc = inc.rename(columns={"compensation_employees": "BQ_ra",
                              "gross_operating_surplus": "BS_ebe", "mixed_income": "BT_im"})
    out = out.merge(inc[["date", "BQ_ra", "BS_ebe", "BT_im"]], on="date", how="left")
    for c in ("BQ_ra", "BS_ebe", "BT_im"):
        fuentes[c] = "data/processed/dane_gdp_income_colombia.csv"

    inv = pd.read_csv(PROCESSED_DIR / "dane_gdp_expenditure_colombia.csv", parse_dates=["date"])
    inv["date"] = _inicio_trimestre_a_cierre(inv["date"])
    out = out.merge(inv[["date", "investment"]].rename(columns={"investment": "BE_inv"}), on="date", how="left")
    fuentes["BE_inv"] = "data/processed/dane_gdp_expenditure_colombia.csv"

    # VA de petróleo y gas, impuestos menos subvenciones: no entran al cálculo
    # vigente (producto total, alpha CBO), pero el motor las exige para fechar T
    # (último trimestre con insumos completos).  Desde el repo si existe el
    # archivo de --pib-insumos-v7; si no, de la referencia.
    repo_v7 = PROCESSED_DIR / "dane_pib_insumos_v7_trimestral.csv"
    if repo_v7.exists() and {"X_vapg", "BR_tms"} <= set(pd.read_csv(repo_v7, nrows=0).columns):
        ins = pd.read_csv(repo_v7, parse_dates=["date"])
        ins["date"] = _inicio_trimestre_a_cierre(ins["date"])
        out = out.merge(ins[["date", "X_vapg", "BR_tms"]], on="date", how="left")
        for c in ("X_vapg", "BR_tms"):
            fuentes[c] = "data/processed/dane_pib_insumos_v7_trimestral.csv"
    else:
        out = out.merge(ref[["date", "X_vapg", "BR_tms"]], on="date", how="left")
        for c in ("X_vapg", "BR_tms"):
            fuentes[c] = "REFERENCIA legacy/v7/datos/insumos_pib_trimestral.csv (falta scraper)"

    cols = ["date", "V_pib", "X_vapg", "BE_inv", "BQ_ra", "BR_tms", "BS_ebe", "BT_im"]
    return out[cols].sort_values("date").reset_index(drop=True), fuentes


def _mensual_repo() -> pd.DataFrame:
    lab = pd.read_csv(PROCESSED_DIR / "dane_labor_colombia.csv", parse_dates=["date"])
    return pd.DataFrame({
        "date": lab["date"],
        "pet": lab["pet_thousands"] * 1000.0,
        "fl": (lab["occupied_thousands"] + lab["unemployed_thousands"]) * 1000.0,
        "ocup": lab["occupied_thousands"] * 1000.0,
    }).sort_values("date").reset_index(drop=True)


def _hc_repo() -> pd.DataFrame:
    p = pd.read_csv(PROCESSED_DIR / "pwt_colombia.csv")
    return p[["year", "human_capital"]].rename(columns={"human_capital": "hc"}).dropna()


# --------------------------------------------------------------------------- #
#  API                                                                         #
# --------------------------------------------------------------------------- #
def preparar_raiz(raiz: Path, modo: str = "repo") -> Dict[str, str]:
    """Crea ``raiz`` con todos los insumos del motor v7. Devuelve {insumo: fuente}."""
    if modo not in MODOS:
        raise ValueError(f"modo debe ser uno de {MODOS}, no {modo!r}")
    raiz = Path(raiz)
    # Se sobrescribe archivo por archivo (sin borrar la carpeta): todos los
    # insumos se reescriben en cada corrida.
    (raiz / "datos").mkdir(parents=True, exist_ok=True)
    (raiz / "outputs").mkdir(exist_ok=True)
    filtros_previo = raiz / FILTROS.name
    if not FILTROS.exists() and filtros_previo.exists():
        filtros_previo.rename(raiz / "outputs" / f"_obsoleto_{FILTROS.name}")
    datos = raiz / "datos"
    f: Dict[str, str] = {}

    f["festivos"] = _copiar(FESTIVOS, raiz / FESTIVOS.name)
    f["capital"] = _copiar(CAPITAL, datos / "dane_stock_capital_productivo.csv")
    if FILTROS.exists():
        f["filtros (paso 3)"] = _copiar(FILTROS, raiz / FILTROS.name)
    else:
        f["filtros (paso 3)"] = "NO DISPONIBLE: se omite el contraste con VIOG"

    if modo == "referencia":
        f["Data_NAIRU.xlsx"] = _copiar(REF_DATA_NAIRU, raiz / "Data_NAIRU.xlsx")
        f["trm"] = _copiar(REF_TRM, raiz / REF_TRM.name)
        for p in sorted(REF_DATOS.glob("*.csv")):
            if p.name != "dane_stock_capital_productivo.csv":
                f[p.name] = _copiar(p, datos / p.name)
        return f

    # ---- modo repo ----
    repo_dn = INPUTS_DIR / "Data_NAIRU.xlsx"
    ok, motivo = data_nairu_cumple_v7(repo_dn)
    if ok:
        f["Data_NAIRU.xlsx"] = _copiar(repo_dn, raiz / "Data_NAIRU.xlsx")
    else:
        logger.warning("data/inputs/Data_NAIRU.xlsx no sirve para v7 (%s); se usa la referencia.", motivo)
        f["Data_NAIRU.xlsx"] = (f"REFERENCIA ({motivo}): "
                                + _copiar(REF_DATA_NAIRU, raiz / "Data_NAIRU.xlsx"))

    trm_repo = PROCESSED_DIR / "trm_cop_usd_promedio_mensual.csv"
    if trm_repo.exists():
        pd.read_csv(trm_repo)[["date", "trm_cop_usd"]].to_csv(raiz / REF_TRM.name, index=False)
        f["trm"] = "data/processed/trm_cop_usd_promedio_mensual.csv"
    else:
        f["trm"] = "REFERENCIA (falta en data/processed): " + _copiar(REF_TRM, raiz / REF_TRM.name)

    ref_trim = pd.read_csv(REF_DATOS / "insumos_pib_trimestral.csv", parse_dates=["date"])
    trim, ft = _trimestral_repo(ref_trim)
    trim.to_csv(datos / "insumos_pib_trimestral.csv", index=False)
    f.update({f"insumos_pib_trimestral.{k}": v for k, v in ft.items()})

    _mensual_repo().to_csv(datos / "insumos_pib_mensual.csv", index=False)
    f["insumos_pib_mensual.csv"] = "data/processed/dane_labor_colombia.csv"
    _hc_repo().to_csv(datos / "insumos_pib_hc_anual.csv", index=False)
    f["insumos_pib_hc_anual.csv"] = "data/processed/pwt_colombia.csv"

    for nombre, cols in MENSUALES_LAB.items():
        p = PROCESSED_DIR / nombre
        if p.exists() and set(cols) <= set(pd.read_csv(p, nrows=0).columns):
            f[nombre] = _copiar(p, datos / nombre)
        else:
            f[nombre] = "REFERENCIA (falta scraper): " + _copiar(REF_DATOS / nombre, datos / nombre)
    return f
