"""Script temporal: verificar fechas extraídas vs esperadas con nuevo algoritmo."""
from pathlib import Path
from src.sources.andi.eoic import EOICParser

cases = [
    ("Informe EOIC Febrero 2017.pdf", "2017-02"),
    ("Informe EOIC Agosto 2017.pdf", "2017-08"),
    ("Informe EOIC Septiembre 2017.pdf", "2017-09"),
    ("EOIC Reporte-Enero 2018 VF.pdf", "2018-01"),
    ("Informe EOIC Febrero 2018_636609448481285303.pdf", "2018-02"),
    ("Informe EOIC Mayo 2019_636997537573126562.pdf", "2019-05"),
    ("Infografía EOIC - Marzo 2020_637280022429026435.pdf", "2020-03"),
    ("Informe EOIC Julio 2021.pdf", "2021-07"),
    ("Informe EOIC Octubre 2022.pdf", "2022-10"),
    ("Informe EOIC Octubre 2023.pdf", "2023-10"),
    ("Informe EOIC Marzo 2024.pdf", "2024-03"),
    ("Informe EOIC Junio 2024.pdf", "2024-06"),
    ("Informe EOIC Marzo 2025.pdf", "2025-03"),
    ("Informe EOIC Junio 2025.pdf", "2025-06"),
    ("Informe EOIC - ANDI - Mayo_638929178751992479.pdf", None),  # no year in filename
]

base = Path("data/raw/andi/")

print("=== Test extract_date_from_filename ===")
for pdf_name, expected in cases:
    got = EOICParser.extract_date_from_filename(pdf_name)
    match = "OK" if got == expected else "MISMATCH"
    print(f"{match}  expected={expected}  got={got}  <- {pdf_name}")

print()
print("=== Test extract_date_from_content (new algorithm) ===")
for pdf_name, expected_fn in cases:
    p = base / pdf_name
    parser = EOICParser(p)
    _ = parser._get_text()
    date_content = parser.extract_date_from_content()
    # We expect content date to match filename date (or be reasonable)
    print(f"filename_date={expected_fn}  content_date={date_content}  <- {pdf_name}")
