# legacy/v7 — script de referencia v7

Aquí va `pib_potencial_integrado_v7.py` (el script monolítico que produce los
resultados del working paper v7, `docs/paper/working_paper_v7.pdf`).

Igual que con v3 (`legacy/pib_potencial_integrado_v3.py`), este archivo es la
**especificación ejecutable**: no se edita. El pipeline (`src/`) se alinea con
él y los tests comparan contra sus salidas congeladas en
`tests/fixtures/baseline_v7/`.

Cuando llegue el `.py`:

1. Copiarlo aquí con el nombre `pib_potencial_integrado_v7.py`.
2. Seguir la lista de verificación de `docs/integracion_v7.md`.
