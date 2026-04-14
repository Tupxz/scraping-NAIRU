"""Debug: infografías sin año para determinar el mes real."""
from pathlib import Path
from src.sources.andi.eoic import EOICParser

pdfs = [
    "Infografía EOIC - Abril.pdf",
    "Infografía EOIC - Mayo.pdf",
]

base = Path("data/raw/andi/")
for name in pdfs:
    p = base / name
    parser = EOICParser(p)
    text = parser._get_text()
    # Print first 500 chars to understand the date context
    print(f"=== {name} ===")
    snippet = text[:600].replace("\n", " | ")
    print(snippet)
    print()
    # Also try to find year mentions
    import re
    years = re.findall(r"20[12]\d", text[:2000])
    print(f"Years found in first 2000 chars: {years}")
    print()
