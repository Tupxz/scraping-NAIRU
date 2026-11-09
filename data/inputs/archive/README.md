# data/inputs/archive

Insumos manuales que ya no lee ningún módulo de `src/`:

- `nairu_estimates_v6.csv` — salida del modelo v6; de aquí salió el histórico de ICU (`data/inputs/icu_historical.csv`, que lo cita como fuente).
- `Kalman_filter.xlsx`, `Code1.do` — material del modelo original del profesor Álvaro (Stata/Excel).
- `Empleo/Data_base.xlsx` — base de empleo anterior a la GEIH scrapeada.
- `Data_NAIRU_2026-05-11_icu2001.xlsx` — versión del commit `a30fb4b` (2026-05-11), recuperada del historial de git el 2026-11-09. Es el insumo con que se generó la línea base v7: su `Unemp_Desest` (TD desestacionalizada publicada por el DANE, 1 decimal) coincide **exacto** con `tests/fixtures/baseline_v7/nairu_estimates_v7.csv` en los 264 meses, y su `ICU` cubre **2001-01 → 2025-12** (2004+ idéntico a `icu_historical.csv` a 5e-5). El STL de v7 sobre ese ICU reproduce el ICU desestacionalizado de la línea base a 8e-5.
