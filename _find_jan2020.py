"""Buscar enero 2020 en cache."""
import json
with open("data/raw/andi/processed_cache.json") as f:
    cache = json.load(f)
for k, v in cache.items():
    if "Enero 2020" in k or v.get("date") == "2020-01":
        print(f"{k} -> {v}")
    if "Enero%202020" in k:
        print(f"{k} -> {v}")

print(f"\nTotal entries in cache: {len(cache)}")
dates = sorted(set(v["date"] for v in cache.values() if "date" in v))
print(f"2020 dates: {[d for d in dates if d.startswith('2020')]}")
