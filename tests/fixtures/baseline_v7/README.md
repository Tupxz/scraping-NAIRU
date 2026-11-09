# Línea base v7 (salidas congeladas del script de referencia)

Archivos entregados con el working paper v7 (2026-10-22), **reemplazados por la actualización del 2026-11-09** (NAIRU dic-2025 = 8,626 %, NAICU = 77,530 %, igual al paper; la versión de octubre daba 8,88 % / 77,49 %). Son la referencia
numérica que el pipeline automático debe replicar.

| Archivo | Frecuencia | Cobertura | Contenido |
|---|---|---|---|
| `nairu_estimates_v7.csv` | mensual | 2004-01 → 2025-12 (264) | NAIRU, NAICU (constante), ICU y desempleo observados, IC 95 % de ambas |
| `pib_potencial_integrado_v7.xlsx` | — | — | Excel de salida del script (hojas de resumen) |
| `pib_potencial_integrado_v7_series.csv` | trimestral | 2004Q1 → 2026Q1 (89) | 60 columnas del módulo de función de producción: insumos (`V_pib`, `pet`, `fl`, `ocup`, `K`…), TGP*, horas, índices `idx_*`, α, PTF, `pib_pot`, `brecha_pot`, comparadores HP |

Convención de fechas: el CSV trimestral etiqueta cada trimestre con el mes de
**cierre** (2004-03-01 = 2004Q1). El repo usa el mes de **inicio** (2004-01-01);
alinear por `(año, trimestre)`, no por fecha.

No editar a mano. Si el script v7 se corrige, regenerar ambos archivos desde él.
