"""Punto de entrada principal del pipeline NAIRU Colombia.

Uso:
    python -m src.main                # Solo desempleo (default)
    python -m src.main --unemployment # Solo desempleo
    python -m src.main --ipc          # Solo IPC (DANE real)
    python -m src.main --all          # Ambos pipelines
"""

from __future__ import annotations

import argparse
import sys
import time

from src.io_utils import ensure_directories, setup_logging
from src.quality_checks import QualityCheckError


def run_pipeline(
    run_unemployment: bool = True,
    run_ipc: bool = False,
) -> None:
    """Ejecuta los pipelines seleccionados."""
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("PIPELINE NAIRU COLOMBIA — Inicio")
    logger.info("=" * 60)

    start_time = time.time()

    try:
        ensure_directories()
        logger.info("Directorios verificados.")

        if run_unemployment:
            from src.pipelines import run_unemployment as unemp_pipeline
            unemp_pipeline.run()

        if run_ipc:
            from src.pipelines import run_ipc as ipc_pipeline
            ipc_pipeline.run()

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETADO en %.2f segundos", elapsed)
        logger.info("=" * 60)

    except QualityCheckError as e:
        logger.error("ERROR DE CALIDAD: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("ERROR INESPERADO: %s", e)
        sys.exit(1)


def main() -> None:
    """Punto de entrada CLI."""
    parser = argparse.ArgumentParser(description="Pipeline NAIRU Colombia")
    parser.add_argument(
        "--ipc", action="store_true",
        help="Ejecutar solo el pipeline IPC (DANE real)",
    )
    parser.add_argument(
        "--unemployment", action="store_true",
        help="Ejecutar solo el pipeline de desempleo",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecutar ambos pipelines",
    )
    args = parser.parse_args()

    if args.all:
        run_pipeline(run_unemployment=True, run_ipc=True)
    elif args.ipc:
        run_pipeline(run_unemployment=False, run_ipc=True)
    elif args.unemployment:
        run_pipeline(run_unemployment=True, run_ipc=False)
    else:
        run_pipeline(run_unemployment=True, run_ipc=False)


if __name__ == "__main__":
    main()
