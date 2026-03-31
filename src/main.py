"""Punto de entrada principal del pipeline NAIRU Colombia.

Uso:
    python -m src.main                # Solo desempleo (default)
    python -m src.main --unemployment # Solo desempleo
    python -m src.main --ipc          # Solo IPC (DANE real)
    python -m src.main --banrep       # Solo inflación (BANREP/SUAMECA)
    python -m src.main --brent        # Solo Brent (FRED/EIA)
    python -m src.main --andi         # Solo ANDI EOIC (incremental)
    python -m src.main --andi-backfill # ANDI EOIC (backfill completo)
    python -m src.main --all          # Todos los pipelines
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
    run_banrep: bool = False,
    run_brent: bool = False,
    run_andi: bool = False,
    andi_backfill: bool = False,
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

        if run_banrep:
            from src.pipelines import run_banrep_inflation as banrep_pipeline
            banrep_pipeline.run()

        if run_brent:
            from src.pipelines import run_brent as brent_pipeline
            brent_pipeline.run()

        if run_andi:
            from src.pipelines import run_andi as andi_pipeline
            andi_pipeline.run(backfill=andi_backfill)

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
        "--banrep", action="store_true",
        help="Ejecutar solo el pipeline de inflación (BANREP/SUAMECA)",
    )
    parser.add_argument(
        "--brent", action="store_true",
        help="Ejecutar solo el pipeline de Brent (FRED/EIA)",
    )
    parser.add_argument(
        "--andi", action="store_true",
        help="Ejecutar solo el pipeline ANDI EOIC (incremental)",
    )
    parser.add_argument(
        "--andi-backfill", action="store_true",
        help="Ejecutar pipeline ANDI EOIC en modo backfill (todos los PDFs)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecutar todos los pipelines",
    )
    args = parser.parse_args()

    # --all activa todos los pipelines.
    if args.all:
        run_pipeline(
            run_unemployment=True, run_ipc=True,
            run_banrep=True, run_brent=True,
            run_andi=True,
        )
        return

    # Si se pasa --andi-backfill, implica --andi con backfill.
    use_andi = args.andi or args.andi_backfill

    # Si no se pasa ningún flag, ejecutar desempleo por defecto.
    any_selected = (
        args.unemployment or args.ipc or args.banrep
        or args.brent or use_andi
    )
    if not any_selected:
        run_pipeline(run_unemployment=True)
        return

    # Se pueden combinar flags libremente.
    run_pipeline(
        run_unemployment=args.unemployment,
        run_ipc=args.ipc,
        run_banrep=args.banrep,
        run_brent=args.brent,
        run_andi=use_andi,
        andi_backfill=args.andi_backfill,
    )


if __name__ == "__main__":
    main()
