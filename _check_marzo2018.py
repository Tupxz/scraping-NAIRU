"""Verificar 88.4% en Marzo 2018 PDF."""
import pdfplumber, re

path = "data/raw/andi/Informe EOIC Marzo 2018.pdf"
with pdfplumber.open(path) as pdf:
    for i, page in enumerate(pdf.pages[:3]):
        text = page.extract_text() or ""
        lower = text.lower()
        # Buscar 88.4 o 88,4
        for m in re.finditer(r"88[.,]4", lower):
            start = max(0, m.start() - 150)
            end = min(len(lower), m.end() + 150)
            print(f"--- Page {i}, '88.4' at pos {m.start()} ---")
            print(text[start:end])
            print()
        # Buscar porcentajes cerca de 'capacidad'
        for m in re.finditer(r"(\d{2}[.,]\d)\s*%", text):
            pos = m.start()
            region = lower[max(0, pos - 100) : pos + 50]
            if "capacidad" in region or "instalada" in region:
                print(f"PCT near 'capacidad': {m.group()} pag {i} pos {pos}")
                print(text[max(0, pos - 100) : pos + 100])
                print()
