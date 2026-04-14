"""Debug Abril 2025."""
from pathlib import Path
from src.sources.andi.eoic import EOICParser, _PERCENT_RE
import re

p = Path("data/raw/andi/Informe EOIC Abril 2025_638869591383024863.pdf")
parser = EOICParser(p)
text = parser._get_text()

# Find the main capacidad instalada section
norm = parser._normalize(text)
idx = norm.find("capacidad instalada en ab")
if idx > 0:
    region = text[max(0, idx - 50):idx + 500]
    print("=== Region around 'capacidad instalada en abril 2025' ===")
    print(region)
    print()

# Find ALL mentions of "se situo" or "se ubico" near percentages
for m in re.finditer(r"se\s+situ[oó]\s+en\s+(\d[\d,\.]+)\s*%", text, re.IGNORECASE):
    print(f"'se situó en': {m.group(1)}% at pos {m.start()}")

print()
# Find the fuzzy match position
norm_phrase = parser._normalize("utilizacion de la capacidad instalada")
best_pos = -1
best_score = 0.0
from difflib import SequenceMatcher
window = len(norm_phrase)
for i in range(len(norm) - window + 1):
    chunk = norm[i:i+window]
    score = SequenceMatcher(None, norm_phrase, chunk).ratio()
    if score > best_score:
        best_score = score
        best_pos = i

print(f"Best fuzzy match at pos {best_pos}, score={best_score:.3f}")
region = text[max(0, best_pos - 50):best_pos + 300]
print("=== Region from fuzzy match ===")
print(repr(region[:400]))
