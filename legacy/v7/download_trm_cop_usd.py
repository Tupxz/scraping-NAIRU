"""
download_trm_cop_usd.py
=======================

Descarga la Tasa Representativa del Mercado (TRM, pesos colombianos por dolar)
certificada por la Superintendencia Financiera de Colombia y publicada como dato
abierto en datos.gov.co (conjunto "Tasa de Cambio Representativa del Mercado -
TRM", id 32sa-8pi3; es la misma serie que difunde el Banco de la Republica) y
construye el PROMEDIO MENSUAL 2001-01 en adelante.

Cada registro trae un valor y una vigencia [vigenciadesde, vigenciahasta] (la TRM
de los viernes rige sabado, domingo y festivos). El promedio mensual se calcula
expandiendo cada vigencia a dias calendario y promediando los dias del mes, que
es la convencion del "promedio mensual" del Banco de la Republica.

Salida: trm_cop_usd_promedio_mensual.csv con columnas date (primer dia del mes),
trm_cop_usd (promedio mensual), n_dias (dias calendario promediados).

Uso:
  py -3.10 download_trm_cop_usd.py [--output trm_cop_usd_promedio_mensual.csv]
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

SOURCE_BASE_URL = "https://www.datos.gov.co/resource/32sa-8pi3.json"
START_DATE = date(2000, 12, 1)   # un mes antes de la muestra para cubrir vigencias
DEFAULT_OUTPUT = "trm_cop_usd_promedio_mensual.csv"


def build_source_url(start_date: date, limit: int = 50000) -> str:
    params = {
        "$limit": str(limit),
        "$where": f"vigenciadesde >= '{start_date.isoformat()}'",
        "$order": "vigenciadesde",
    }
    return f"{SOURCE_BASE_URL}?{urlencode(params, quote_via=quote)}"


def fetch_records(url: str) -> list[dict]:
    req = Request(url, headers={"Accept": "application/json",
                                "User-Agent": "Mozilla/5.0 TRMDownloader/1.0"})
    with urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def monthly_average(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["desde"] = pd.to_datetime(df["vigenciadesde"]).dt.normalize()
    df["hasta"] = pd.to_datetime(df["vigenciahasta"]).dt.normalize()
    df = df.dropna(subset=["valor", "desde", "hasta"]).sort_values("desde")
    # expandir cada vigencia a dias calendario
    days = []
    for v, d0, d1 in zip(df["valor"], df["desde"], df["hasta"]):
        for d in pd.date_range(d0, d1, freq="D"):
            days.append((d, v))
    daily = pd.DataFrame(days, columns=["day", "trm"]).drop_duplicates("day", keep="last")
    daily["date"] = daily["day"].dt.to_period("M").dt.to_timestamp()
    out = daily.groupby("date").agg(trm_cop_usd=("trm", "mean"), n_dias=("trm", "size")).reset_index()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    args = ap.parse_args()
    url = build_source_url(START_DATE)
    print(f"Descargando TRM desde {url}")
    records = fetch_records(url)
    print(f"  {len(records)} registros")
    out = monthly_average(records)
    out = out[out["date"] >= pd.Timestamp("2001-01-01")]
    # el ultimo mes puede estar incompleto: se marca en n_dias (< dias del mes)
    out.to_csv(Path(args.output), index=False, float_format="%.6f")
    print(f"  {len(out)} meses -> {args.output}  ({out['date'].iloc[0].date()} .. {out['date'].iloc[-1].date()})")


if __name__ == "__main__":
    main()
