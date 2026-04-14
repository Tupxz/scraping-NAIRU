"""Script temporal para probar extracción de los 15 PDFs no procesados."""
from pathlib import Path
from src.sources.andi.eoic import EOICParser

failed_pdfs = [
    "Informe EOIC Febrero 2017.pdf",
    "Informe EOIC Agosto 2017.pdf",
    "Informe EOIC Septiembre 2017.pdf",
    "EOIC Reporte-Enero 2018 VF.pdf",
    "Informe EOIC Febrero 2018_636609448481285303.pdf",
    "Informe EOIC Mayo 2019_636997537573126562.pdf",
    "Infografía EOIC - Marzo 2020_637280022429026435.pdf",
    "Informe EOIC Julio 2021.pdf",
    "Informe EOIC Octubre 2022.pdf",
    "Informe EOIC Octubre 2023.pdf",
    "Informe EOIC Marzo 2024.pdf",
    "Informe EOIC Junio 2024.pdf",
    "Informe EOIC Marzo 2025.pdf",
    "Informe EOIC Junio 2025.pdf",
    "Informe EOIC - ANDI - Mayo_638929178751992479.pdf",
]

base = Path("data/raw/andi/")
ok_count = 0
fail_count = 0

for pdf_name in failed_pdfs:
    p = base / pdf_name
    if not p.exists():
        print(f"  NO EXISTE: {pdf_name}")
        fail_count += 1
        continue
    parser = EOICParser(p)
    result = parser.extract_capacity_utilization()
    date = parser.extract_date_from_content()
    if result:
        val, ctx = result
        ok_count += 1
        print(f"OK  {pdf_name}")
        print(f"    valor={val:.1f}%  date_content={date}  ctx={ctx[:80]}")
    else:
        fail_count += 1
        print(f"FAIL  {pdf_name}")
        print(f"    SIN EXTRACCION  date_content={date}")
        text = parser._get_text()
        print(f"    texto_length={len(text)}")
        # Show first 300 chars to understand the content
        snippet = text[:300].replace("\n", " | ")
        print(f"    snippet: {snippet}")
    print()

print(f"\n=== RESUMEN: {ok_count} OK, {fail_count} FAIL de {len(failed_pdfs)} ===")
