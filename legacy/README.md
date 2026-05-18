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
