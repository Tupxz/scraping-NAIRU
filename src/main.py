"""Punto de entrada principal del pipeline NAIRU Colombia.

Uso:
    python -m src.main                  # Solo desempleo (default)
    python -m src.main --unemployment   # Solo desempleo (TD + TGP + PET)
    python -m src.main --pwt            # Solo PWT 10.01 (capital stock + capital humano)
    python -m src.main --dane-gdp       # Solo PIB trimestral DANE (Cuentas Nacionales)
    python -m src.main --ipc            # Solo IPC (DANE real)
    python -m src.main --banrep         # Solo inflación (BANREP/SUAMECA)
    python -m src.main --tes            # Solo TES Cero Cupón (BANREP/SUAMECA)
    python -m src.main --brent          # Solo Brent (FRED/EIA)
    python -m src.main --andi           # Solo ANDI EOIC (incremental)
    python -m src.main --andi-backfill  # ANDI EOIC (backfill completo)
    python -m src.main --andi-reprocess # ANDI EOIC (reprocesar PDFs locales)
    python -m src.main --all            # Todos los pipelines de datos + merge (rápido, ~20s)
    python -m src.main --nairu-dataset  # Construir Data_NAIRU.xlsx desde fuentes del repo
    python -m src.main --nairu-estim    # Estimar NAIRU/NAICU (costoso, requiere --nairu-dataset)
"""

from __future__ import annotations

import argparse
import sys
import time

from src.io_utils import ensure_directories, setup_logging
from src.quality_checks import QualityCheckError


def run_pipeline(
    run_unemployment: bool = True,
    run_pwt: bool = False,
    run_informality: bool = False,
    run_viog: bool = False,
    run_viog_co: bool = False,
    run_dane_gdp: bool = False,
    run_ipc: bool = False,
    run_banrep: bool = False,
    run_tes: bool = False,
    run_brent: bool = False,
    run_andi: bool = False,
    andi_backfill: bool = False,
    andi_reprocess: bool = False,
    run_nairu_dataset: bool = False,
    run_nairu_estimation: bool = False,
    run_merge: bool = False,
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

        if run_pwt:
            from src.pipelines import run_pwt as pwt_pipeline
            pwt_pipeline.run()

        if run_informality:
            from src.pipelines import run_informality as informality_pipeline
            informality_pipeline.run()

        if run_viog:
            from src.pipelines import run_viog as viog_pipeline
            viog_pipeline.run()

        if run_viog_co:
            from src.pipelines import run_viog as viog_pipeline
            viog_pipeline.run_colombia()

        if run_dane_gdp:
            from src.pipelines import run_dane_gdp as dane_gdp_pipeline
            dane_gdp_pipeline.run()

        if run_ipc:
            from src.pipelines import run_ipc as ipc_pipeline
            ipc_pipeline.run()

        if run_banrep:
            from src.pipelines import run_banrep_inflation as banrep_pipeline
            banrep_pipeline.run()

        if run_tes:
            from src.pipelines import run_banrep_tes as tes_pipeline
            tes_pipeline.run()

        if run_brent:
            from src.pipelines import run_brent as brent_pipeline
            brent_pipeline.run()

        if run_andi:
            from src.pipelines import run_andi as andi_pipeline
            andi_pipeline.run(backfill=andi_backfill)

        if andi_reprocess:
            from src.sources.andi.eoic import reprocess_local_pdfs
            reprocess_local_pdfs()

        if run_nairu_dataset:
            from src.pipelines import build_nairu_dataset as nairu_ds_pipeline
            nairu_ds_pipeline.run()

        if run_nairu_estimation:
            from src.pipelines import run_nairu_estimation as nairu_estim_pipeline
            nairu_estim_pipeline.run()

        if run_merge:
            from src.pipelines import run_merge as merge_pipeline
            merge_pipeline.run()

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
        "--viog", action="store_true",
        help="Ejecutar pipeline VIOG-USA (brecha del producto USA, 5 filtros ponderados)",
    )
    parser.add_argument(
        "--viog-co", action="store_true",
        help="Ejecutar pipeline VIOG-Colombia (requiere data/inputs/PIB_CO.xlsx)",
    )
    parser.add_argument(
        "--informality", action="store_true",
        help="Ejecutar pipeline de informalidad laboral (DANE GEIH-EISS, 13 ciudades)",
    )
    parser.add_argument(
        "--pwt", action="store_true",
        help="Ejecutar solo el pipeline PWT 10.01 (capital stock + capital humano)",
    )
    parser.add_argument(
        "--dane-gdp", action="store_true",
        help="Ejecutar pipeline PIB trimestral DANE (Cuentas Nacionales, desestacionalizado)",
    )
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
        "--tes", action="store_true",
        help="Ejecutar solo el pipeline de TES Cero Cupón (BANREP/SUAMECA)",
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
        "--andi-reprocess", action="store_true",
        help="Reprocesar PDFs locales de ANDI que no están en el CSV",
    )
    parser.add_argument(
        "--nairu-dataset", action="store_true",
        help="Construir Data_NAIRU.xlsx desde las fuentes procesadas del repo",
    )
    parser.add_argument(
        "--nairu-estim", action="store_true",
        help="Estimar NAIRU/NAICU con Data_NAIRU.xlsx (requiere --nairu-dataset previo)",
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Unir todas las bases procesadas en nairu_dataset.csv",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecutar todos los pipelines + merge",
    )
    args = parser.parse_args()

    # --all activa todos los pipelines + merge.
    if args.all:
        run_pipeline(
            run_unemployment=True, run_pwt=True,
            run_informality=False,  # excluido: no se usa en el dataset final
            run_viog=True,
            run_viog_co=True,
            run_dane_gdp=True,
            run_ipc=True, run_banrep=True,
            run_tes=True, run_brent=True,
            run_andi=True, andi_reprocess=False,  # reprocess solo con --andi-reprocess explícito
            run_nairu_dataset=True,
            run_nairu_estimation=True,
            run_merge=True,
        )
        return

    # Si se pasa --andi-backfill, implica --andi con backfill.
    use_andi = args.andi or args.andi_backfill

    # Si no se pasa ningún flag, ejecutar desempleo por defecto.
    any_selected = (
        args.unemployment or args.pwt or args.informality or args.viog
        or args.viog_co or args.dane_gdp
        or args.ipc or args.banrep
        or args.tes or args.brent or use_andi
        or args.andi_reprocess or args.nairu_dataset
        or args.nairu_estim or args.merge
    )
    if not any_selected:
        run_pipeline(run_unemployment=True)
        return

    # Se pueden combinar flags libremente.
    run_pipeline(
        run_unemployment=args.unemployment,
        run_pwt=args.pwt,
        run_informality=args.informality,
        run_viog=args.viog,
        run_viog_co=args.viog_co,
        run_dane_gdp=args.dane_gdp,
        run_ipc=args.ipc,
        run_banrep=args.banrep,
        run_tes=args.tes,
        run_brent=args.brent,
        run_andi=use_andi,
        andi_backfill=args.andi_backfill,
        andi_reprocess=args.andi_reprocess,
        run_nairu_dataset=args.nairu_dataset,
        run_nairu_estimation=args.nairu_estim,
        run_merge=args.merge,
    )


if __name__ == "__main__":
    main()
