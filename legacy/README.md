# `legacy/` — código no productivo

Este directorio conserva código histórico que **ya no forma parte del
pipeline reproducible**. Se mantiene por referencia y para reproducir
experimentos del periodo de prototipado, no para uso en producción.

## `legacy/andi_agent/`

Agente standalone para procesar PDFs de la encuesta EOIC de ANDI con
parser regex + fallback a LLM local vía Ollama. **Sustituido por**
`src/sources/andi/eoic.py`, que está integrado al pipeline, tiene
tests offline (`tests/test_andi.py`) y respeta el patrón de tres
capas. No usar `legacy/andi_agent/` para alimentar `nairu_dataset.csv`.

## `legacy/pib_potencial_integrado_v3.py`

Monolito de 3.831 líneas, escrito fuera del repo, que integra en un solo
archivo el motor NAIRU/NAICU (Kalman biestado + suavizado RTS + toda la
calibración) y un motor de PIB potencial con **PTF estructural estilo CBO**
—regresión sobre nudos Bry-Boschan, condicionada a la brecha de
desempleo— en reemplazo de la PTF por Boosted-HP del pipeline actual.

**No se importa desde `src/`.** Se versiona por dos razones:

1. Es la **especificación** de la migración descrita en
   `docs/prompts/prompt_fable5_pib_potencial_v3.md`.
2. Es la **línea base numérica**: la Fase 0 de esa migración consiste en
   correrlo tal cual y congelar sus salidas en
   `tests/fixtures/baseline_v3/`; el criterio de aceptación de todo el
   port es reproducirlas con `rtol=1e-10`.

Cuando la migración termine y los tests de equivalencia pasen, este
archivo queda como referencia histórica, igual que `andi_agent/`.
