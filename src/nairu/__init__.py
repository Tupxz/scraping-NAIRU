"""Paquete de estimación NAIRU/NAICU.

Estructura
----------
src/nairu/
  __init__.py         — este archivo
  estimation.py       — orquestador v6 (refactor de nairu_estimation_v6.py)
  model_core.py       — modelo estadístico (nairu_estimation_v5 promovido)
                        ← AGREGAR ESTE ARCHIVO con el código del modelo

El archivo ``model_core.py`` debe exponer las siguientes funciones::

    load_and_prepare_data(data_path: Path) -> pd.DataFrame
    build_model_data(data: pd.DataFrame) -> dict
    estimate_parameters(model_data: dict) -> object
    build_outputs(data, model_data, fit, output_dir: Path) -> tuple[Path, str]
"""
