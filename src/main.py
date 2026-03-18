"""Punto de entrada principal del pipeline NAIRU Colombia.

Orquesta las tres etapas del pipeline:
1. Extracción  — Descarga de datos crudos del DANE
2. Transformación — Limpieza y estandarización
3. Validación — Controles de calidad
4. Carga — Almacenamiento del dataset procesado

Uso:
    python -m src.main
"""

from __future__ import annotations

import sys
import time

from src.dane import (
    clean_unemployment_data,
    download_raw_data,
    save_processed_data,
)
from src.io_utils import ensure_directories, setup_logging
from src.quality_checks import QualityCheckError, run_all_checks


def run_pipeline() -> None:
    """Ejecuta el pipeline completo de datos."""
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("PIPELINE NAIRU COLOMBIA — Inicio")
    logger.info("=" * 60)

    start_time = time.time()

    try:
        # Paso 0: Preparar directorios
        ensure_directories()
        logger.info("Directorios verificados.")

        # Paso 1: Extracción
        logger.info("── Etapa 1: EXTRACCIÓN ──")
        raw_path = download_raw_data()

        # Paso 2: Transformación
        logger.info("── Etapa 2: TRANSFORMACIÓN ──")
        df_clean = clean_unemployment_data(raw_path)

        # Paso 3: Validación
        logger.info("── Etapa 3: VALIDACIÓN ──")
        run_all_checks(df_clean)

        # Paso 4: Carga
        logger.info("── Etapa 4: CARGA ──")
        output_path = save_processed_data(df_clean)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETADO en %.2f segundos", elapsed)
        logger.info("Dataset final: %s (%d filas)", output_path, len(df_clean))
        logger.info("=" * 60)

    except QualityCheckError as e:
        logger.error("ERROR DE CALIDAD: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("ERROR INESPERADO: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
