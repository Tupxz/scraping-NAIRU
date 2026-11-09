# -*- coding: utf-8 -*-
"""
construir_salarios_emmet.py -- serie salarial de la industria (DANE, EMMET) para v7.

Fuente: datos/anex-EMMET-TotalNacional-ago2026.xlsx, hoja "9. Enlace legal hasta
2014", fila de dominio T_IND (total industria), base 2018=100.

En el anexo, la columna "Total Sueldos" pertenece al bloque "Sueldos y salarios
REALES" (el anexo territorial lo rotula explicitamente). La serie salarial que
usaban v6 y la primera v7 (datos/emmet_salarios_industria_mensual.csv) es esa
misma columna y se trataba como nominal. Aqui se toma como lo que es -- un indice
REAL -- y se reconstruye el nominal multiplicando por el IPC total (BanRep), que es
el deflactor de la EMMET:
    W_nominal_t = W_real_t * IPC_t        =>  (1 + w_t) = (1 + omega_t)(1 + pi_t)

Salida: datos/emmet_salarios_v7.csv con, por mes,
  idx_sueldos_real, idx_empleo, idx_horas      (anexo, base 2018=100)
  salario_real_por_ocupado = 100*idx_sueldos_real/idx_empleo
  ipc                                         (BanRep, fin de mes llevado a inicio)
  salario_nominal_por_ocupado = salario_real_por_ocupado * ipc / 100
  var_anual_salario_real, var_anual_salario_nominal   (%, interanual)

Uso:  py -3.10 construir_salarios_emmet.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

AQUI = Path(__file__).resolve().parent
ANEXO = AQUI / "datos" / "anex-EMMET-TotalNacional-ago2026.xlsx"
IPC_CSV = AQUI / "datos" / "banrep_ipc_indice_mensual.csv"
SALIDA = AQUI / "datos" / "emmet_salarios_v7.csv"
HOJA = "9. Enlace legal hasta 2014"


def leer_anexo() -> pd.DataFrame:
    raw = pd.read_excel(ANEXO, sheet_name=HOJA, header=None)
    fila_enc = raw.index[raw.iloc[:, 0].astype(str).str.strip() == "Dominios"][0]
    enc = [" ".join(str(c).split()) for c in raw.iloc[fila_enc]]   # el anexo trae dobles espacios
    datos = raw.iloc[fila_enc + 1:].copy()
    datos = datos[datos.iloc[:, 0].astype(str).str.strip() == "T_IND"]

    def col(clave: str) -> int:
        idx = [i for i, c in enumerate(enc) if clave.lower() in c.lower()]
        if not idx:
            raise KeyError(f"No se halló la columna '{clave}' en {HOJA}: {enc}")
        return idx[0]

    out = pd.DataFrame({
        "date": pd.to_datetime(dict(year=datos.iloc[:, 1].astype(int), month=datos.iloc[:, 2].astype(int), day=1)),
        "idx_sueldos_real": pd.to_numeric(datos.iloc[:, col("Total Sueldos")], errors="coerce").to_numpy(),
        "idx_empleo": pd.to_numeric(datos.iloc[:, col("Total Empleo")], errors="coerce").to_numpy(),
        "idx_horas": pd.to_numeric(datos.iloc[:, col("Horas Totales")], errors="coerce").to_numpy(),
    })
    return out.dropna(subset=["idx_sueldos_real"]).sort_values("date").reset_index(drop=True)


def main() -> None:
    w = leer_anexo()
    ipc = pd.read_csv(IPC_CSV)
    ipc.columns = ["date", "ipc"]
    ipc["date"] = pd.to_datetime(ipc["date"]).dt.to_period("M").dt.to_timestamp()
    w = w.merge(ipc, on="date", how="left")
    w["salario_real_por_ocupado"] = 100.0 * w["idx_sueldos_real"] / w["idx_empleo"]
    w["salario_nominal_por_ocupado"] = w["salario_real_por_ocupado"] * w["ipc"] / 100.0
    for c, n in (("salario_real_por_ocupado", "var_anual_salario_real"),
                 ("salario_nominal_por_ocupado", "var_anual_salario_nominal")):
        w[n] = 100.0 * (w[c] / w[c].shift(12) - 1.0)
    w.to_csv(SALIDA, index=False)

    # Control: la serie que usaban v6 y la v7 inicial debe ser la columna real del anexo
    viejo = pd.read_csv(AQUI / "datos" / "emmet_salarios_industria_mensual.csv", parse_dates=["date"])
    m = viejo.merge(w, on="date")
    dif = (m["var_anual_salario_por_ocupado"] - m["var_anual_salario_real"]).abs()
    print(f"{SALIDA.name}: {len(w)} meses, {w['date'].min():%Y-%m} a {w['date'].max():%Y-%m}; "
          f"IPC disponible hasta {w.dropna(subset=['ipc'])['date'].max():%Y-%m}")
    print(f"Control vs serie anterior (var. anual): max |dif| = {dif.max():.4f} pp en {int(dif.notna().sum())} meses "
          f"(revisiones del anexo de ago-2026)")
    v = w.dropna(subset=["var_anual_salario_nominal"])
    print(f"Media 2015-2025: salario real {v['var_anual_salario_real'].mean():.2f}%  "
          f"nominal {v['var_anual_salario_nominal'].mean():.2f}%")


if __name__ == "__main__":
    main()
