"""Benchmark: ¿dónde se gasta el tiempo procesando PDFs ANDI?"""
import time, pdfplumber
from pathlib import Path
from difflib import SequenceMatcher

RAW = Path("data/raw/andi")
pdfs = sorted(RAW.glob("*.pdf"))
# Solo tomar los informes grandes (no infografías)
pdfs = [p for p in pdfs if "Informe" in p.name][:5]

for pdf_path in pdfs:
    print(f"\n{'='*60}")
    print(f"PDF: {pdf_path.name}")
    
    # 1. Abrir PDF
    t0 = time.perf_counter()
    with pdfplumber.open(pdf_path) as pdf:
        t_open = time.perf_counter() - t0
        
        # 2. Extraer texto
        t1 = time.perf_counter()
        pages_text = []
        for page in pdf.pages:
            pages_text.append(page.extract_text() or "")
        t_extract = time.perf_counter() - t1
        
        total_chars = sum(len(t) for t in pages_text)
        n_pages = len(pdf.pages)
    
    # 3. Simular fuzzy matching (SequenceMatcher) — lo que hace _strategy_text
    t2 = time.perf_counter()
    full_text = "\n".join(pages_text).lower()
    keywords = ["uso de capacidad", "utilización de capacidad", "capacidad instalada"]
    n_comparisons = 0
    for kw in keywords:
        step = max(1, len(kw) // 2)
        for i in range(0, len(full_text) - len(kw), step):
            chunk = full_text[i:i+len(kw)+10]
            ratio = SequenceMatcher(None, kw, chunk).ratio()
            n_comparisons += 1
            if ratio > 0.85:
                pass  # found
    t_fuzzy = time.perf_counter() - t2
    
    total = t_open + t_extract + t_fuzzy
    print(f"  Páginas: {n_pages}, Chars: {total_chars:,}")
    print(f"  Abrir PDF:       {t_open:.3f}s ({t_open/total*100:.0f}%)")
    print(f"  Extraer texto:   {t_extract:.3f}s ({t_extract/total*100:.0f}%)")
    print(f"  Fuzzy matching:  {t_fuzzy:.3f}s ({t_fuzzy/total*100:.0f}%) — {n_comparisons:,} comparaciones")
    print(f"  TOTAL:           {total:.3f}s")
