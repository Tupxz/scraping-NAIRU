"""Corrección puntual: infografías sin año asignadas a meses incorrectos."""
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from src.config import PROCESSED_DIR, RAW_ANDI_DIR, ANDI_CONFIG

csv_path = PROCESSED_DIR / ANDI_CONFIG.processed_filename
cache_path = RAW_ANDI_DIR / ANDI_CONFIG.cache_filename

# Leer cache
with open(cache_path) as f:
    cache = json.load(f)

# Corrección 1: "Infografía EOIC - Abril.pdf" es abril 2020, no enero 2020
key_abril = "local://Infografía EOIC - Abril.pdf"
if key_abril in cache:
    old = cache[key_abril]
    print(f"Abril: {old['date']} -> 2020-04  (val={old['value']})")
    cache[key_abril]["date"] = "2020-04"

# Corrección 2: "Infografía EOIC - Mayo.pdf" es mayo 2020
# Este no está en el cache porque fue "omitido" (su fecha 2020-01 ya existía).
# Lo procesamos de nuevo.
from src.sources.andi.eoic import EOICParser

mayo_pdf = RAW_ANDI_DIR / "Infografía EOIC - Mayo.pdf"
parser = EOICParser(mayo_pdf)
result = parser.extract_capacity_utilization()
if result:
    val, ctx = result
    print(f"Mayo 2020: valor={val}%  ctx={ctx[:80]}")
    cache["local://Infografía EOIC - Mayo.pdf"] = {
        "status": "ok",
        "date": "2020-05",
        "value": val,
        "context": ctx,
    }

# Guardar cache corregido
cache_path.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
print("Cache actualizado.")

# Reconstruir CSV desde cache
records = []
for key, entry in cache.items():
    if entry.get("status") != "ok" or "value" not in entry or "date" not in entry:
        continue
    date_str = entry["date"]
    parts = date_str.split("-")
    records.append({
        "date": f"{date_str}-01",
        "year": int(parts[0]),
        "month": int(parts[1]),
        "capacity_utilization": entry["value"],
        "source": key,
        "download_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    })

df = pd.DataFrame(records)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# Deduplicar por fecha (mantener la primera)
df = df.drop_duplicates(subset="date", keep="first")

from src.config import ANDI_PROCESSED_COLUMNS
df = df[ANDI_PROCESSED_COLUMNS]
df.to_csv(csv_path, index=False)
print(f"CSV guardado: {len(df)} filas")

# Verificar
print(f"\nRango: {df['date'].min()} → {df['date'].max()}")
print(f"Min: {df['capacity_utilization'].min():.1f}%  Max: {df['capacity_utilization'].max():.1f}%  Media: {df['capacity_utilization'].mean():.1f}%")

all_months = pd.date_range(df["date"].min(), df["date"].max(), freq="MS")
existing = set(df["date"])
missing = [d for d in all_months if d not in existing]
print(f"Meses esperados: {len(all_months)}, Presentes: {len(df)}, Faltantes: {len(missing)}")
for m in missing:
    print(f"  {m.strftime('%Y-%m')}")
