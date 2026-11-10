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

## `legacy/v7/` — especificación de referencia del modelo vigente

- `pib_potencial_integrado_v7.py`: script del documento metodológico
  (`docs/paper/working_paper_v7.pdf`). **No se edita.** El pipeline lo usa a
  través de `src/v7/motor.py`, una copia literal en la que solo cambia la raíz
  de insumos y salidas (`V7_ROOT`); si el script cambia, se regenera con
  `python scripts/sync_motor_v7.py` (un test exige que la copia siga siendo literal).
- `datos/`, `construir_salarios_emmet.py`, `download_trm_cop_usd.py`,
  `trm_cop_usd_promedio_mensual.csv`: insumos y scripts con que se generó la
  línea base del documento. El modo `python -m src.pipelines.run_v7 --referencia`
  los usa para reproducir sus cifras exactas (`tests/test_v7.py`); el pipeline
  automático usa en cambio los datos del scraping.

## Historia

- **v3** (`pib_potencial_integrado_v3.py`, Kalman biestado + PTF por tramos) se
  integró en septiembre de 2026 y se retiró el 2026-11-10 al pasar a v7
  (CHANGELOG [0.8.0]); sigue disponible en el historial de git.
