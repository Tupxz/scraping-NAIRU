"""Script temporal: inspeccionar los 4 PDFs con valores sospechosamente bajos."""
from pathlib import Path
from src.sources.andi.eoic import EOICParser

suspects = [
    "Informe EOIC Abril 2019.pdf",
    "Informe EOIC Mayo 2024.pdf",
    "Informe EOIC Septiembre 2024.pdf",
    "Informe EOIC Abril 2025_638869591383024863.pdf",
]

base = Path("data/raw/andi/")
for pdf_name in suspects:
    p = base / pdf_name
    parser = EOICParser(p)
    result = parser.extract_capacity_utilization()
    if result:
        val, ctx = result
        print(f"{pdf_name}")
        print(f"  valor={val:.1f}%  ctx={ctx[:120]}")
    # Also show raw text around "capacidad" for manual verification
    text = parser._get_text()
    import re
    for m in re.finditer(r"capacidad.{0,100}", text.lower()):
        snippet = text[m.start():m.start()+150].replace("\n", " ")
        print(f"  raw: ...{snippet}...")
    print()
