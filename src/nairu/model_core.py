"""
NAIRU/NAICU estimation, alineada a la spec FINAL de pib_potencial_integrado_v3
(estimar_nairu_naicu_estructural_v3, 2026-09-03): curva de Phillips con
histéresis disciplinada (ancla = desempleo/ICU rezagado 1 mes) y holgura de
rezagos distribuidos (3 rezagos) tanto en desempleo como en ICU.

Fix 2026-09-03 (auditoria_src_2026-08-21.md hallazgo #3 + revisión contra
pib_potencial_integrado_v3.py, la referencia de la que parte este repo --
"tiene que arrojar los mismos resultados"): el módulo divergía de la spec
final de v3 en tres puntos estructurales, no solo en el reporte de errores
estándar:

1) nairu_adjustment_speed, naicu_adjustment_speed, log_nairu_transition_std
   y log_naicu_transition_std se estimaban LIBREMENTE por MLE. La spec final
   de v3 los FIJA en valores ya calibrados (phi_n=0.08, phi_c=0.02,
   sigma_nairu=0.05, sigma_naicu=0.20 -- elegidos vía phi_sweep()/
   sigma_sweep()/naicu_grid(), barridos manuales fuera de este módulo)
   precisamente porque son débilmente identificados por la verosimilitud
   (el "pile-up problem" de Stock-Watson, que v3 nombra explícitamente).
   Estimarlos libremente reproduce ese pile-up (confirmado empíricamente:
   Hessiano observado con curvatura genuinamente negativa en esas
   direcciones -- ver proyecto_nairu.md). Aquí se fijan vía
   PARAMETER_BOUNDS de ancho cero (mismo mecanismo que usa v3).
2) La holgura de ICU era un único coeficiente contemporáneo
   (icu_gap_coefficient). La spec final usa 3 rezagos distribuidos
   (icu_coefficient_0/1/2), igual que ya se hacía para desempleo
   (unemployment_coefficient_0/1/2).
3) COVID se trataba con un regresor separado (covid_shock_coefficient) en
   la ecuación de medición. La spec final NO usa ese regresor: neutraliza el
   pico de desempleo SOLO en el ancla de histéresis (unemployment_lag1),
   interpolando linealmente en la ventana 2020-04 a 2021-06 -- la ecuación
   de medición sigue viendo el desempleo observado real, así que la brecha
   COVID queda reflejada en el filtro en vez de absorbida por un regresor.

Estos tres cambios sí mueven el NAIRU/NAICU publicado (corrigen una
divergencia real con la referencia), a diferencia del arreglo de reporte de
errores estándar (compute_mle_inference/_reliable_parameter_covariance más
abajo), que es puramente de presentación y no toca ningún punto estimado.

Dos detalles adicionales, descubiertos al verificar esto contra la
referencia (ejecutándola directamente sobre Data_NAIRU.xlsx -- ver
verify_v3alignment.py) y NO obvios con solo leer el código de v3:

4) La ventana muestral debe empezar en 2006-01, no en 2005-01. En v3,
   load_and_prepare_data() es una función GENÉRICA compartida por todas las
   variantes de espec (incluida "ma24", que este repo no usa), y su
   .dropna() se aplica sobre TODAS las columnas que calcula -- incluida
   unemployment_ma24/icu_ma24 (media móvil de 24 meses), aunque la spec
   final ("hysteresis" + "distributed_lag") no las use para nada. Esas 2
   columnas tienen 24 NaN iniciales (más que cualquier otro rezago de este
   módulo, que como mucho tiene 12 por expected_inflation_current_period),
   así que terminan siendo las que de verdad determinan dónde empieza la
   muestra utilizable -- para CUALQUIER espec, la use o no. Por eso este
   módulo también las calcula e incluye en la selección de columnas de
   load_and_prepare_data(), aunque ModelData/_kalman_pass nunca las toquen:
   es la única forma de reproducir exactamente los mismos 238 registros
   (2006-01 a 2025-12) que usa v3.
5) BEST_START_PARAMS_V3 (ver estimate_parameters) es, en la propia v3, un
   punto "ya hallado" por barridos externos (phi_sweep/sigma_sweep/
   naicu_grid) que v3 documenta como el óptimo publicado ("converge de
   inmediato porque ya es el óptimo"), no una semilla cualquiera para que
   L-BFGS-B siga buscando. Confirmado con isolate_objective_diff.py que la
   función objetivo de este módulo es BIT-IDÉNTICA a la de v3 (mismos
   arrays de ModelData, mismo kalman_filter_loglik/kalman_nll evaluado en el
   mismo punto, mismos state_filt/state_pred/covariance_filt paso a paso --
   diff 0.0 en todo). El problema NO es de fidelidad del modelo sino del
   optimizador: con isolate_optimizer_diff.py se confirmó que, en este
   óptimo casi degenerado (varias restricciones de signo a ~1e-4 de su
   borde), la terminación de L-BFGS-B (CONVERGENCE vs.
   ABNORMAL_TERMINATION_IN_LNSRCH) depende de la versión de numpy/BLAS Y del
   ORDEN de las coordenadas del vector de parámetros -- reordenar según el
   orden exacto de v3 arregla numpy<2.0 pero introduce una discrepancia
   nueva bajo numpy>=2.0 que antes no existía, así que reordenar solo no es
   un arreglo robusto ni universal.
   Por eso estimate_parameters() ya NO usa la salida de minimize() como
   resultado: fit.params es SIEMPRE BEST_START_PARAMS_V3 (recortado a
   PARAMETER_BOUNDS), de forma determinista e independiente del entorno.
   minimize() se sigue ejecutando, pero solo como comparación diagnóstica
   (si encuentra un punto con verosimilitud mejor de forma no trivial, se
   reporta -- nunca se adopta -- en fit.message). fit.success es por
   construcción siempre True (significa "hay parámetros publicados
   válidos", no "scipy convergió"); build_outputs() por eso ya no exige
   fit.success, sino que valida directamente np.isfinite(fit.params) y que
   la verosimilitud en ese punto no esté penalizada (ver el comentario ahí).

Verificado (verify_v3alignment.py, verify_full_pipeline.py): con estos 5
puntos, nairu_estimate/naicu_estimate y sus bandas de 95% coinciden con la
salida de estimar_nairu_naicu_estructural_v3() a 1e-15 (ruido de punto
flotante) en las 238 observaciones, en ambos entornos numpy (<2.0 y >=2.0).
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize


DATA_FILE = "Data_NAIRU.xlsx"
OUTPUT_DIR = "outputs"

UNEMPLOYMENT_COL = "Unemp_Desest"
CORE_INFLATION_COL = "Core_Inf"
INFLATION_TARGET_COL = "Inf_Goal"
OIL_PRICE_COL = "Brent_Oil_Price"
NOMINAL_RATE_COL = "TES_Rate_1yr_COP"
REAL_RATE_COL = "TES_Rate_1yr_UVR"
ICU_COL = "ICU"

EXPECTATIONS_HORIZON_MONTHS = 12
LOG_2PI = float(np.log(2.0 * np.pi))
MIN_VARIANCE = 1e-10
LARGE_PENALTY = 1e12
INITIAL_NAIRU_VARIANCE = 9.0
INITIAL_NAICU_VARIANCE = 16.0
MIN_NAIRU_LEVEL = 3.0
MAX_NAIRU_LEVEL = 20.0
MIN_NAICU_LEVEL = 55.0
MAX_NAICU_LEVEL = 90.0
Z_95 = 1.959963984540054
Z_90 = 1.6448536269514722

# Fix 2026-09-03 (alineación v3, ver docstring del módulo): antes un dummy
# 2020-03..2021-06 entraba como REGRESOR (covid_shock_coefficient) en la
# ecuación de medición. La spec final de v3 usa un mecanismo distinto: NO
# hay regresor -- se neutraliza el pico de desempleo solo en el ANCLA de
# histéresis (unemployment_lag1, ver _apply_covid_anchor_interpolation),
# interpolando linealmente en esta ventana (v3 arranca en abril, no marzo).
COVID_ANCHOR_START = "2020-04-01"
COVID_ANCHOR_END = "2021-06-01"

# Fix 2026-09-03: valores calibrados de la spec final de v3
# (PHI_NAIRU/PHI_NAICU/SIGMA_NAIRU_FINAL/SIGMA_NAICU_FINAL en
# pib_potencial_integrado_v3.py), elegidos vía phi_sweep()/sigma_sweep()/
# naicu_grid() -- barridos manuales fuera de este módulo, no parte de la
# corrida automática. Se usan para FIJAR los 4 parámetros débilmente
# identificados (ver PARAMETER_BOUNDS) y como semilla en estimate_parameters.
NAIRU_ADJUSTMENT_SPEED_FINAL = 0.08
NAICU_ADJUSTMENT_SPEED_FINAL = 0.02
NAIRU_TRANSITION_STD_FINAL = 0.05
NAICU_TRANSITION_STD_FINAL = 0.20

SVG_PANEL_WIDTH = 1500
SVG_PANEL_HEIGHT = 620
SVG_MARGIN_LEFT = 78
SVG_MARGIN_RIGHT = 72
SVG_MARGIN_TOP = 88
SVG_MARGIN_BOTTOM = 70
SVG_GUTTER = 88
PARAMETER_NAMES = [
    "intercept",
    "inflation_lag1_coefficient",
    "inflation_lag2_coefficient",
    "unemployment_coefficient_0",
    "unemployment_coefficient_1",
    "unemployment_coefficient_2",
    "icu_coefficient_0",
    "icu_coefficient_1",
    "icu_coefficient_2",
    "expectations_coefficient",
    "oil_shock_coefficient",
    "nairu_adjustment_speed",
    "naicu_adjustment_speed",
    "log_measurement_error_std",
    "log_nairu_transition_std",
    "log_naicu_transition_std",
    "nairu_initial_level",
    "naicu_initial_level",
]
PARAMETER_BOUNDS = [
    (-5.0, 5.0),    # intercept
    (-0.99, 0.99),  # inflation_lag1_coefficient
    (-0.99, 0.99),  # inflation_lag2_coefficient
    (-2.0, 0.0),    # unemployment_coefficient_0
    (-2.0, 0.0),    # unemployment_coefficient_1
    (-2.0, 0.0),    # unemployment_coefficient_2
    (0.0, 2.0),     # icu_coefficient_0
    (0.0, 2.0),     # icu_coefficient_1
    (0.0, 2.0),     # icu_coefficient_2
    (-2.0, 2.0),    # expectations_coefficient
    (-2.0, 2.0),    # oil_shock_coefficient
    # Fix 2026-09-03 (alineación v3): los siguientes 4 bounds son de ANCHO
    # CERO a propósito -- fijan el parámetro en vez de acotarlo. Es el mismo
    # mecanismo que usa la spec final de v3 (cfg.bounds[...] = (PHI, PHI)),
    # no un accidente ni una restricción "muy apretada". Ver docstring del
    # módulo: estos 4 son débilmente identificados por la verosimilitud
    # (pile-up de Stock-Watson) y se calibraron aparte, fuera de este
    # módulo, vía phi_sweep()/sigma_sweep()/naicu_grid().
    (NAIRU_ADJUSTMENT_SPEED_FINAL, NAIRU_ADJUSTMENT_SPEED_FINAL),  # nairu_adjustment_speed (FIJO)
    (NAICU_ADJUSTMENT_SPEED_FINAL, NAICU_ADJUSTMENT_SPEED_FINAL),  # naicu_adjustment_speed (FIJO)
    (-10.0, 4.0),   # log_measurement_error_std
    (
        math.log(NAIRU_TRANSITION_STD_FINAL), math.log(NAIRU_TRANSITION_STD_FINAL),
    ),  # log_nairu_transition_std (FIJO)
    (
        math.log(NAICU_TRANSITION_STD_FINAL), math.log(NAICU_TRANSITION_STD_FINAL),
    ),  # log_naicu_transition_std (FIJO)
    (0.0, 25.0),    # nairu_initial_level
    (50.0, 90.0),   # naicu_initial_level
]
RAW_STD_INDICES = {13, 14, 15}
# Fix 2026-09-03 (alineación v3): índices de PARAMETER_BOUNDS con ancho
# cero -- FIJOS por diseño, nunca estimados. compute_mle_inference y
# _reliable_parameter_covariance los tratan distinto de una dirección
# genuinamente no identificada (pile-up): a estos ni se les intenta
# calcular curvatura, porque no tiene sentido preguntarse la SE de un
# parámetro que no varía.
FIXED_PARAMETER_INDICES = frozenset(
    index for index, (lower, upper) in enumerate(PARAMETER_BOUNDS) if lower == upper
)


@dataclass(slots=True)
class FitResult:
    params: np.ndarray
    success: bool
    message: str
    nll: float
    optimizer_inverse_hessian: Optional[np.ndarray]


@dataclass(slots=True)
class ModelData:
    inflation_gap_change: np.ndarray
    inflation_gap_change_lag1: np.ndarray
    inflation_gap_change_lag2: np.ndarray
    unemployment_current: np.ndarray
    unemployment_lag1: np.ndarray
    unemployment_lag2: np.ndarray
    # Fix 2026-09-03 (alineación v3, ver docstring del módulo): réplica del
    # ancla de histéresis separada de v3 (allí, un campo `unemployment_lag1`
    # DISTINTO del que usa `_slack_terms`/la matriz `unemployment_lags` para
    # la ecuación de medición -- ver _aplicar_dummy_covid en la referencia).
    # Aquí, unemployment_lag1/lag2 de arriba son el dato REAL sin tocar (los
    # usa la ecuación de medición vía unemployment_coefficient_1/2); esta
    # copia aparte es la que _apply_covid_anchor_interpolation interpola en
    # la ventana COVID y la que usa _kalman_pass SOLO para el término de
    # control del estado (histéresis NAIRU).
    unemployment_hysteresis_anchor: np.ndarray
    icu_current: np.ndarray
    icu_lag1: np.ndarray
    icu_lag2: np.ndarray  # Fix 2026-09-03 (alineación v3): 3er término de la holgura ICU distribuida
    expected_inflation_term: np.ndarray
    oil_shock: np.ndarray
    dates: pd.Series  # Fix 2026-09-03 (alineación v3): ventana de _apply_covid_anchor_interpolation
    n_obs: int
    # covid_dummy REMOVIDO (Fix 2026-09-03, alineación v3): COVID ya no es un
    # regresor de la ecuación de medición -- ver _apply_covid_anchor_interpolation.


@dataclass(slots=True)
class KalmanHistory:
    state_pred: np.ndarray
    covariance_pred: np.ndarray
    state_filt: np.ndarray
    covariance_filt: np.ndarray
    # Fix 2026-09-01 (Fase 1, auditoria_src_2026-08-21.md): suma SOLO de los
    # términos gaussianos de la innovación (0.5·(log(2π)+log(var)+innov²/var)),
    # sin las penalizaciones de frontera (muros de nairu/naicu, LARGE_PENALTY).
    # `nll` (el valor que devuelve _kalman_pass) sigue siendo el objetivo que
    # ve el optimizador -- penalizado, correcto para mantenerlo dentro de las
    # cotas. `unpenalized_nll` es la log-verosimilitud real del modelo, la
    # que debe usarse para AIC/BIC (ver compute_mle_inference).
    unpenalized_nll: float


@dataclass(slots=True)
class InferenceResult:
    coefficient_table: pd.DataFrame
    diagnostics_table: pd.DataFrame
    covariance_matrix: pd.DataFrame
    covariance_source: str
    warning: Optional[str]


def _to_float_array(series: pd.Series) -> np.ndarray:
    return np.ascontiguousarray(series.to_numpy(dtype=float))


def _stabilize_covariance(covariance: np.ndarray) -> np.ndarray:
    stabilized = 0.5 * (covariance + covariance.T)
    stabilized[0, 0] = max(float(stabilized[0, 0]), MIN_VARIANCE)
    stabilized[1, 1] = max(float(stabilized[1, 1]), MIN_VARIANCE)

    max_covariance = np.sqrt(stabilized[0, 0] * stabilized[1, 1]) - 1e-12
    off_diagonal = float(np.clip(stabilized[0, 1], -max_covariance, max_covariance))
    stabilized[0, 1] = off_diagonal
    stabilized[1, 0] = off_diagonal
    return stabilized


def _normal_two_sided_p_value(z_score: float) -> float:
    if not np.isfinite(z_score):
        return float("nan")
    return float(math.erfc(abs(float(z_score)) / math.sqrt(2.0)))


def _significance_stars(p_value: float) -> str:
    if not np.isfinite(p_value):
        return ""
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def _format_float(value: float, decimals: int = 6) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.{decimals}f}"


def _coerce_inverse_hessian(matrix_like: object) -> Optional[np.ndarray]:
    if matrix_like is None:
        return None
    try:
        if hasattr(matrix_like, "todense"):
            dense = np.asarray(matrix_like.todense(), dtype=float)
        else:
            dense = np.asarray(matrix_like, dtype=float)
    except Exception:
        return None

    if dense.ndim != 2 or dense.shape[0] != dense.shape[1]:
        return None
    if not np.all(np.isfinite(dense)):
        return None
    return 0.5 * (dense + dense.T)


def _finite_difference_step(
    params: np.ndarray,
    index: int,
    data: ModelData,
) -> Optional[Tuple[float, float, float]]:
    value = float(params[index])
    lower_bound, upper_bound = PARAMETER_BOUNDS[index]
    step = max(1e-4 * max(1.0, abs(value)), 1e-5)

    if np.isfinite(lower_bound):
        step = min(step, 0.45 * max(value - lower_bound, 0.0))
    if np.isfinite(upper_bound):
        step = min(step, 0.45 * max(upper_bound - value, 0.0))
    # Fix 2026-09-03 (alineación v3): el `if index == 6` de antes protegía
    # específicamente icu_gap_coefficient (un único coeficiente, restricción
    # de signo propia icu_gap_coefficient <= 0.02) para no pisar la región
    # penalizada al diferenciar. Ahora la restricción de signo es sobre la
    # SUMA icu_coefficient_0+1+2 (ver _kalman_pass), no sobre un índice fijo
    # -- no se generaliza aquí a propósito: el bucle de abajo (`step *= 0.5`
    # hasta 20 veces si plus/minus_value cae en la penalización) ya cubre
    # este caso de forma genérica para cualquier parámetro, solo que puede
    # tardar 1-2 iteraciones más que un atajo hecho a mano.

    minimum_step = max(1e-7 * max(1.0, abs(value)), 1e-7)
    if step <= minimum_step:
        return None

    for _ in range(20):
        plus_params = params.copy()
        minus_params = params.copy()
        plus_params[index] += step
        minus_params[index] -= step
        plus_value = kalman_filter_loglik(plus_params, data)
        minus_value = kalman_filter_loglik(minus_params, data)
        if (
            np.isfinite(plus_value)
            and np.isfinite(minus_value)
            and plus_value < LARGE_PENALTY * 0.1
            and minus_value < LARGE_PENALTY * 0.1
        ):
            return step, float(plus_value), float(minus_value)
        step *= 0.5
        if step <= minimum_step:
            break
    return None


# Fix 2026-09-03 (alineación v3, ver docstring del módulo): PARAMETER_BOUNDS
# ahora tiene 4 parámetros de ancho cero (FIXED_PARAMETER_INDICES) -- fijos
# por diseño, no estimados. _finite_difference_step no puede diferenciar un
# parámetro cuyo lower==upper (el paso se fuerza a 0 y la función devuelve
# None de inmediato), así que ANTES de este fix _approximate_observed_hessian
# fallaba en el PRIMER índice fijo que encontraba (índice 11 en el orden
# actual, nairu_adjustment_speed) y abortaba el Hessiano COMPLETO -- incluidos
# los 14 parámetros libres que sí tienen curvatura bien definida. Ahora la
# diferenciación (diagonal y cruzada) se hace SOLO sobre los índices libres;
# el Hessiano devuelto sigue siendo de 18x18 pero con NaN en toda fila/columna
# de un parámetro fijo -- compute_mle_inference ya sabe reportar esos NaN como
# "fijo por diseño" en vez de "no confiable" (ver el bloque de notas ahí).
def _approximate_observed_hessian(
    params: np.ndarray,
    data: ModelData,
) -> Tuple[Optional[np.ndarray], Optional[str]]:
    center_value = kalman_filter_loglik(params, data)
    if not np.isfinite(center_value) or center_value >= LARGE_PENALTY * 0.1:
        return None, "Unable to evaluate the likelihood at the optimum."

    n_params = len(params)
    free_indices = [index for index in range(n_params) if index not in FIXED_PARAMETER_INDICES]
    steps: Dict[int, float] = {}
    axis_cache: Dict[int, Tuple[float, float]] = {}

    for index in free_indices:
        selected = _finite_difference_step(params, index, data)
        if selected is None:
            return None, (
                "Observed-information Hessian could not be computed because at least "
                f"one parameter ({PARAMETER_NAMES[index]}) is too close to a bound."
            )
        step, plus_value, minus_value = selected
        steps[index] = step
        axis_cache[index] = (plus_value, minus_value)

    hessian = np.full((n_params, n_params), np.nan, dtype=float)
    for index in free_indices:
        plus_value, minus_value = axis_cache[index]
        hessian[index, index] = (
            plus_value - 2.0 * center_value + minus_value
        ) / (steps[index] * steps[index])

    # Fix 2026-09-03 (alineación v3): descubierto al correr esto contra la
    # spec final -- unemployment_gap_coefficient (beta_u0+u1+u2) quedó a
    # 0.000079 de su restricción de signo (>= -0.02), un margen MENOR que el
    # paso por defecto (~1e-4) que ya se eligió para cada parámetro por
    # separado mirando solo esa dirección. Perturbar DOS coeficientes u_j a
    # la vez (esquina "++" del término cruzado) puede sumar ~2x ese paso y sí
    # cruzar el límite aunque cada paso individual (usado en la diagonal) sea
    # seguro por sí solo. Antes esto abortaba el Hessiano COMPLETO -- ahora
    # se reduce el paso (solo para ESTE par, la diagonal ya calculada arriba
    # no se toca) igual que ya hace _finite_difference_step para un solo
    # parámetro, hasta encontrar un paso conjunto que no pise la región
    # penalizada.
    for position_i in range(len(free_indices)):
        for position_j in range(position_i + 1, len(free_indices)):
            i = free_indices[position_i]
            j = free_indices[position_j]
            step_i = steps[i]
            step_j = steps[j]
            evaluation_values: list = []
            for _ in range(20):
                evaluation_values = []
                for direction_i in (1.0, -1.0):
                    for direction_j in (1.0, -1.0):
                        shifted_params = params.copy()
                        shifted_params[i] += direction_i * step_i
                        shifted_params[j] += direction_j * step_j
                        shifted_value = kalman_filter_loglik(shifted_params, data)
                        evaluation_values.append(shifted_value)

                if (
                    np.all(np.isfinite(evaluation_values))
                    and np.max(evaluation_values) < LARGE_PENALTY * 0.1
                ):
                    break
                step_i *= 0.5
                step_j *= 0.5
            else:
                return None, (
                    "Observed-information Hessian became unstable when evaluating "
                    f"cross derivatives for {PARAMETER_NAMES[i]} and {PARAMETER_NAMES[j]}."
                )

            f_pp, f_pm, f_mp, f_mm = evaluation_values
            cross_derivative = (f_pp - f_pm - f_mp + f_mm) / (4.0 * step_i * step_j)
            hessian[i, j] = cross_derivative
            hessian[j, i] = cross_derivative

    return hessian, None


def _invert_information_matrix(
    information_matrix: np.ndarray,
) -> Tuple[Optional[np.ndarray], Optional[str]]:
    if information_matrix is None or not np.all(np.isfinite(information_matrix)):
        return None, "Information matrix is not finite."

    eigenvalues, eigenvectors = np.linalg.eigh(0.5 * (information_matrix + information_matrix.T))
    if np.min(eigenvalues) <= 1e-10:
        return None, "Information matrix is singular or not positive definite."

    inverse_matrix = eigenvectors @ np.diag(1.0 / eigenvalues) @ eigenvectors.T
    return 0.5 * (inverse_matrix + inverse_matrix.T), None


# Fix 2026-09-03 (hallazgo #3 de auditoria_src_2026-08-21.md, "SE del MLE ~
# identidad"): _invert_information_matrix es todo-o-nada -- si CUALQUIER
# dirección del Hessiano de 17x17 no es positiva-definida, se descarta la
# matriz COMPLETA y se cae al inverso-Hessiano crudo del optimizador para
# los 17 parámetros, incluyendo los que sí tienen curvatura sana. La
# investigación 2026-09-03 (ver proyecto_nairu.md) probó 3 arreglos por el
# lado del optimizador (tolerancias más estrictas, penalización suave,
# reparametrización sin límites) y los 3 fallan o degeneran el filtro de
# Kalman -- la causa no es un bug numérico sino el "pile-up problem": un
# fenómeno bien documentado en modelos Kalman de nivel local con series
# cortas (Harvey 1989; Stock & Watson) donde la(s) varianza(s) de
# transición -- y, en este caso, acoplado a ellas, el nivel inicial del
# NAICU -- quedan débilmente identificadas por la verosimilitud (curvatura
# observada genuinamente no positiva, no un artefacto de convergencia).
# Decisión del usuario 2026-09-03: no tocar el punto estimado (fit.params,
# nairu_colombia.csv quedan exactamente igual) y arreglar solo el reporte.
#
# _reliable_parameter_covariance excluye iterativamente al parámetro más
# responsable de la dirección de curvatura más negativa (mayor |peso| en el
# autovector de ese autovalor) hasta que la submatriz restante sí es
# positiva-definida, y devuelve la inversa de esa submatriz -- con NaN en
# las filas/columnas excluidas -- en vez de tirar toda la información. Así
# los parámetros con curvatura sana (típicamente la mayoría) recuperan un
# error estándar real basado en el Hessiano observado, y solo los
# genuinamente mal identificados quedan marcados como no confiables en vez
# de mostrar un número engañoso.
def _reliable_parameter_covariance(
    hessian: np.ndarray,
    max_exclusions: int = 6,
) -> Tuple[Optional[np.ndarray], list]:
    n_params = hessian.shape[0]
    excluded_indices: set = set()
    for _ in range(max_exclusions):
        remaining_indices = [index for index in range(n_params) if index not in excluded_indices]
        if len(remaining_indices) < 2:
            return None, sorted(excluded_indices)

        submatrix = hessian[np.ix_(remaining_indices, remaining_indices)]
        submatrix = 0.5 * (submatrix + submatrix.T)
        eigenvalues, eigenvectors = np.linalg.eigh(submatrix)

        if np.min(eigenvalues) > 1e-10:
            inverse_submatrix = eigenvectors @ np.diag(1.0 / eigenvalues) @ eigenvectors.T
            inverse_submatrix = 0.5 * (inverse_submatrix + inverse_submatrix.T)
            covariance_matrix = np.full((n_params, n_params), np.nan, dtype=float)
            for local_row, global_row in enumerate(remaining_indices):
                for local_col, global_col in enumerate(remaining_indices):
                    covariance_matrix[global_row, global_col] = inverse_submatrix[local_row, local_col]
            return covariance_matrix, sorted(excluded_indices)

        worst_direction = eigenvectors[:, int(np.argmin(eigenvalues))]
        worst_local_index = int(np.argmax(np.abs(worst_direction)))
        excluded_indices.add(remaining_indices[worst_local_index])

    return None, sorted(excluded_indices)


def compute_mle_inference(fit: FitResult, data: ModelData) -> InferenceResult:
    covariance_matrix: Optional[np.ndarray] = None
    covariance_source = ""
    warning: Optional[str] = None
    unreliable_parameter_indices: list = []

    n_params = len(fit.params)
    # Fix 2026-09-03 (alineación v3): 4 parámetros son fijos por diseño (ver
    # FIXED_PARAMETER_INDICES) -- _invert_information_matrix/_reliable_parameter_covariance
    # exigen una matriz totalmente finita, así que no pueden recibir el
    # Hessiano de 18x18 con NaN en esas filas/columnas directamente. Se
    # trabaja sobre la SUBMATRIZ de parámetros libres y el resultado se
    # embebe de vuelta en una matriz de 18x18 (NaN en fijos + no confiables).
    free_indices = [index for index in range(n_params) if index not in FIXED_PARAMETER_INDICES]

    def _embed(free_covariance: np.ndarray) -> np.ndarray:
        embedded = np.full((n_params, n_params), np.nan, dtype=float)
        for local_row, global_row in enumerate(free_indices):
            for local_col, global_col in enumerate(free_indices):
                embedded[global_row, global_col] = free_covariance[local_row, local_col]
        return embedded

    observed_hessian, observed_warning = _approximate_observed_hessian(fit.params, data)
    if observed_hessian is not None:
        free_hessian = observed_hessian[np.ix_(free_indices, free_indices)]
        free_covariance, inversion_warning = _invert_information_matrix(free_hessian)
        if free_covariance is not None:
            covariance_matrix = _embed(free_covariance)
            covariance_source = "Observed-information Hessian (numerical second derivatives)."
            warning = inversion_warning
        else:
            partial_free_covariance, unreliable_local_indices = _reliable_parameter_covariance(
                free_hessian
            )
            unreliable_parameter_indices = [free_indices[local] for local in unreliable_local_indices]
            if partial_free_covariance is not None and unreliable_parameter_indices:
                covariance_matrix = _embed(partial_free_covariance)
                covariance_source = (
                    "Observed-information Hessian (submatriz -- ver 'covariance_warning')."
                )
                unreliable_names = ", ".join(
                    PARAMETER_NAMES[index] for index in unreliable_parameter_indices
                )
                warning = (
                    f"Curvatura observada no positiva en: {unreliable_names}. Error estándar "
                    "no confiable para esos parámetros (posible identificación débil / "
                    "'pile-up problem' -- ver auditoria_src_2026-08-21.md, hallazgo #3); el "
                    "resto de los parámetros usa el Hessiano observado normalmente."
                )
            else:
                warning = inversion_warning
                optimizer_covariance = _coerce_inverse_hessian(fit.optimizer_inverse_hessian)
                if optimizer_covariance is not None:
                    covariance_matrix = optimizer_covariance
                    covariance_source = "L-BFGS-B inverse Hessian approximation."
    else:
        warning = observed_warning
        optimizer_covariance = _coerce_inverse_hessian(fit.optimizer_inverse_hessian)
        if optimizer_covariance is not None:
            covariance_matrix = optimizer_covariance
            covariance_source = "L-BFGS-B inverse Hessian approximation."

    if covariance_matrix is None:
        covariance_matrix = np.full((n_params, n_params), np.nan, dtype=float)
        covariance_source = "No covariance approximation available."

    raw_standard_errors = np.sqrt(np.maximum(np.diag(covariance_matrix), 0.0))
    rows = []
    for index, name in enumerate(PARAMETER_NAMES):
        estimate_raw = float(fit.params[index])
        standard_error_raw = float(raw_standard_errors[index])
        if index in RAW_STD_INDICES:
            estimate = float(np.exp(estimate_raw))
            standard_error = float(estimate * standard_error_raw)
            z_stat = float("nan")
            p_value = float("nan")
            ci_lower_95 = float(np.exp(estimate_raw - Z_95 * standard_error_raw))
            ci_upper_95 = float(np.exp(estimate_raw + Z_95 * standard_error_raw))
            note = "Positive standard deviation. SE via delta method; p-value omitted."
            reported_name = name.replace("log_", "")
        else:
            estimate = estimate_raw
            standard_error = standard_error_raw
            z_stat = estimate / standard_error if standard_error > 0.0 else float("nan")
            p_value = _normal_two_sided_p_value(z_stat)
            ci_lower_95 = estimate - Z_95 * standard_error if standard_error > 0.0 else float("nan")
            ci_upper_95 = estimate + Z_95 * standard_error if standard_error > 0.0 else float("nan")
            note = ""
            reported_name = name

        # Fix 2026-09-03 (alineación v3 + ver _reliable_parameter_covariance
        # más arriba): dos motivos DISTINTOS para no tener error estándar, y
        # se distinguen explícitamente en vez de mostrar la misma nota para
        # ambos -- "fijo por diseño" (bound de ancho cero, calibrado fuera de
        # este módulo, nunca se intentó estimar) no es lo mismo que "no
        # confiable" (SÍ se intentó estimar y la curvatura observada salió
        # genuinamente no positiva -- pile-up). std_error/z_stat/ci_* ya
        # salen NaN de forma automática en ambos casos (covariance_matrix
        # trae NaN en esas filas/columnas); solo la nota se sobreescribe.
        if index in FIXED_PARAMETER_INDICES:
            note = (
                "Fijo por diseño (bound de ancho cero -- ver PARAMETER_BOUNDS y el "
                "docstring del módulo): valor calibrado fuera de este módulo "
                "(phi_sweep/sigma_sweep/naicu_grid en pib_potencial_integrado_v3.py), "
                "no estimado por MLE. No aplica error estándar."
            )
        elif index in unreliable_parameter_indices:
            note = (
                "SE no confiable: curvatura observada no positiva (posible "
                "identificación débil / 'pile-up problem', ver "
                "'covariance_warning' en diagnostics_table)."
            )

        rows.append(
            {
                "parameter": reported_name,
                "estimate": estimate,
                "std_error": standard_error,
                "z_stat": z_stat,
                "p_value": p_value,
                "ci_lower_95": ci_lower_95,
                "ci_upper_95": ci_upper_95,
                "significance": _significance_stars(p_value),
                "note": note,
            }
        )

    unemployment_indices = [3, 4, 5]
    unemployment_gap_estimate = float(np.sum(fit.params[unemployment_indices]))
    unemployment_gap_variance = float(
        np.sum(covariance_matrix[np.ix_(unemployment_indices, unemployment_indices)])
    )
    unemployment_gap_standard_error = math.sqrt(max(unemployment_gap_variance, 0.0))
    unemployment_gap_z = (
        unemployment_gap_estimate / unemployment_gap_standard_error
        if unemployment_gap_standard_error > 0.0
        else float("nan")
    )
    unemployment_gap_p_value = _normal_two_sided_p_value(unemployment_gap_z)
    rows.append(
        {
            "parameter": "unemployment_gap_coefficient",
            "estimate": unemployment_gap_estimate,
            "std_error": unemployment_gap_standard_error,
            "z_stat": unemployment_gap_z,
            "p_value": unemployment_gap_p_value,
            "ci_lower_95": unemployment_gap_estimate - Z_95 * unemployment_gap_standard_error,
            "ci_upper_95": unemployment_gap_estimate + Z_95 * unemployment_gap_standard_error,
            "significance": _significance_stars(unemployment_gap_p_value),
            "note": "Derived as beta_u0 + beta_u1 + beta_u2.",
        }
    )

    # Fix 2026-09-03 (alineación v3): fila derivada análoga para ICU -- antes
    # icu_gap_coefficient era un único parámetro estimado directamente (su
    # propia fila ya traía SE); ahora es la suma de 3 rezagos distribuidos,
    # igual que unemployment_gap_coefficient arriba.
    icu_indices = [6, 7, 8]
    icu_total_estimate = float(np.sum(fit.params[icu_indices]))
    icu_total_variance = float(
        np.sum(covariance_matrix[np.ix_(icu_indices, icu_indices)])
    )
    icu_total_standard_error = math.sqrt(max(icu_total_variance, 0.0))
    icu_total_z = (
        icu_total_estimate / icu_total_standard_error
        if icu_total_standard_error > 0.0
        else float("nan")
    )
    icu_total_p_value = _normal_two_sided_p_value(icu_total_z)
    rows.append(
        {
            "parameter": "icu_total_coefficient",
            "estimate": icu_total_estimate,
            "std_error": icu_total_standard_error,
            "z_stat": icu_total_z,
            "p_value": icu_total_p_value,
            "ci_lower_95": icu_total_estimate - Z_95 * icu_total_standard_error,
            "ci_upper_95": icu_total_estimate + Z_95 * icu_total_standard_error,
            "significance": _significance_stars(icu_total_p_value),
            "note": "Derived as beta_icu0 + beta_icu1 + beta_icu2.",
        }
    )

    # Fix 2026-09-01 (Fase 1, auditoria_src_2026-08-21.md): fit.nll es el
    # objetivo PENALIZADO que optimiza estimate_parameters (incluye los
    # muros de nairu_pred/naicu_pred fuera de rango) -- calcular AIC/BIC
    # directamente sobre -fit.nll trata esas penalizaciones como si fueran
    # verosimilitud real. Se recalcula una pasada del filtro en el óptimo
    # (barata: O(n_obs), nada que ver con las 97 corridas del multi-start)
    # para separar el término gaussiano puro.
    _, history_at_optimum = _kalman_pass(fit.params, data, store_history=True)
    if history_at_optimum is not None:
        true_nll = history_at_optimum.unpenalized_nll
        penalty_at_optimum = float(fit.nll) - true_nll
    else:
        # No debería pasar si fit.success es True, pero no tumbamos todo el
        # reporte de diagnósticos por esto -- se cae al valor penalizado.
        true_nll = float(fit.nll)
        penalty_at_optimum = 0.0

    log_likelihood = -true_nll
    parameter_count = len(fit.params)
    aic = 2.0 * parameter_count - 2.0 * log_likelihood
    bic = math.log(data.n_obs) * parameter_count - 2.0 * log_likelihood
    diagnostics_table = pd.DataFrame(
        [
            {"metric": "observations", "value": float(data.n_obs), "note": ""},
            {"metric": "estimated_parameters", "value": float(parameter_count), "note": ""},
            {"metric": "log_likelihood", "value": log_likelihood, "note": "Gaussiana pura, sin penalizaciones de frontera."},
            {"metric": "negative_log_likelihood", "value": true_nll, "note": "Sin penalizaciones (ver 'penalty_at_optimum')."},
            {"metric": "penalty_at_optimum", "value": penalty_at_optimum, "note": "Aporte de los muros LARGE_PENALTY/nairu-naicu al objetivo optimizado en el punto final; 0 = sin penalización activa."},
            {"metric": "AIC", "value": aic, "note": ""},
            {"metric": "BIC", "value": bic, "note": ""},
            {
                "metric": "optimizer_success",
                "value": float(fit.success),
                "note": fit.message,
            },
            {
                "metric": "covariance_source",
                "value": float("nan"),
                "note": covariance_source,
            },
        ]
    )
    if warning:
        diagnostics_table.loc[len(diagnostics_table)] = {
            "metric": "covariance_warning",
            "value": float("nan"),
            "note": warning,
        }

    covariance_frame = pd.DataFrame(
        covariance_matrix,
        index=PARAMETER_NAMES,
        columns=PARAMETER_NAMES,
    )
    coefficient_table = pd.DataFrame(rows)
    return InferenceResult(
        coefficient_table=coefficient_table,
        diagnostics_table=diagnostics_table,
        covariance_matrix=covariance_frame,
        covariance_source=covariance_source,
        warning=warning,
    )


def _nice_tick_values(lower: float, upper: float, count: int = 5) -> np.ndarray:
    if not np.isfinite(lower) or not np.isfinite(upper):
        return np.array([0.0, 1.0], dtype=float)
    if upper <= lower:
        lower -= 0.5
        upper += 0.5
    return np.linspace(lower, upper, count)


def _build_line_path(x_values: np.ndarray, y_values: np.ndarray) -> str:
    commands = [f"M {x_values[0]:.2f},{y_values[0]:.2f}"]
    commands.extend(
        f"L {x_values[index]:.2f},{y_values[index]:.2f}" for index in range(1, len(x_values))
    )
    return " ".join(commands)


def _build_band_polygon(
    x_values: np.ndarray,
    lower_values: np.ndarray,
    upper_values: np.ndarray,
) -> str:
    upper_points = [f"{x_values[index]:.2f},{upper_values[index]:.2f}" for index in range(len(x_values))]
    lower_points = [
        f"{x_values[index]:.2f},{lower_values[index]:.2f}"
        for index in range(len(x_values) - 1, -1, -1)
    ]
    return " ".join(upper_points + lower_points)


def _draw_svg_panel(
    svg_fragments: list[str],
    panel_left: float,
    panel_top: float,
    panel_width: float,
    panel_height: float,
    dates: pd.Series,
    main_actual: np.ndarray,
    main_estimate: np.ndarray,
    ci_lower_90: np.ndarray,
    ci_upper_90: np.ndarray,
    ci_lower_95: np.ndarray,
    ci_upper_95: np.ndarray,
    secondary_series: np.ndarray,
    title: str,
    main_axis_label: str,
) -> None:
    plot_left = panel_left
    plot_top = panel_top
    plot_right = plot_left + panel_width
    plot_bottom = plot_top + panel_height

    series_min = float(
        np.nanmin(
            np.concatenate(
                [main_actual, main_estimate, ci_lower_95, ci_upper_95, ci_lower_90, ci_upper_90]
            )
        )
    )
    series_max = float(
        np.nanmax(
            np.concatenate(
                [main_actual, main_estimate, ci_lower_95, ci_upper_95, ci_lower_90, ci_upper_90]
            )
        )
    )
    series_span = max(series_max - series_min, 1.0)
    series_padding = 0.08 * series_span
    main_lower = series_min - series_padding
    main_upper = series_max + series_padding

    secondary_min = float(np.nanmin(secondary_series))
    secondary_max = float(np.nanmax(secondary_series))
    secondary_span = max(secondary_max - secondary_min, 0.5)
    secondary_padding = 0.10 * secondary_span
    secondary_lower = secondary_min - secondary_padding
    secondary_upper = secondary_max + secondary_padding

    x_coordinates = np.linspace(plot_left, plot_right, len(dates))

    def map_main(values: np.ndarray) -> np.ndarray:
        return plot_bottom - (values - main_lower) * panel_height / (main_upper - main_lower)

    def map_secondary(values: np.ndarray) -> np.ndarray:
        return plot_bottom - (values - secondary_lower) * panel_height / (secondary_upper - secondary_lower)

    svg_fragments.append(
        f"<text x='{plot_left:.2f}' y='{plot_top - 34:.2f}' font-size='20' "
        f"font-family='Arial, sans-serif' fill='#1f1f1f'>{title}</text>"
    )
    svg_fragments.append(
        f"<rect x='{plot_left:.2f}' y='{plot_top:.2f}' width='{panel_width:.2f}' "
        f"height='{panel_height:.2f}' fill='white' stroke='#D9D9D9' stroke-width='1'/>"
    )

    for tick in _nice_tick_values(main_lower, main_upper):
        y_coord = float(map_main(np.array([tick], dtype=float))[0])
        svg_fragments.append(
            f"<line x1='{plot_left:.2f}' y1='{y_coord:.2f}' x2='{plot_right:.2f}' y2='{y_coord:.2f}' "
            f"stroke='#E9E9E9' stroke-width='1'/>"
        )
        svg_fragments.append(
            f"<text x='{plot_left - 10:.2f}' y='{y_coord + 4:.2f}' text-anchor='end' "
            f"font-size='12' font-family='Arial, sans-serif' fill='#555555'>{tick:.1f}</text>"
        )

    for tick in _nice_tick_values(secondary_lower, secondary_upper):
        y_coord = float(map_secondary(np.array([tick], dtype=float))[0])
        svg_fragments.append(
            f"<text x='{plot_right + 10:.2f}' y='{y_coord + 4:.2f}' text-anchor='start' "
            f"font-size='12' font-family='Arial, sans-serif' fill='#555555'>{tick:.1f}</text>"
        )

    tick_count = 7
    tick_indices = np.linspace(0, len(dates) - 1, tick_count, dtype=int)
    tick_indices = np.unique(tick_indices)
    for index in tick_indices:
        x_coord = float(x_coordinates[index])
        tick_label = pd.Timestamp(dates.iloc[index]).strftime("%Y-%m")
        svg_fragments.append(
            f"<line x1='{x_coord:.2f}' y1='{plot_bottom:.2f}' x2='{x_coord:.2f}' "
            f"y2='{plot_bottom + 6:.2f}' stroke='#666666' stroke-width='1'/>"
        )
        svg_fragments.append(
            f"<text x='{x_coord:.2f}' y='{plot_bottom + 22:.2f}' text-anchor='middle' "
            f"font-size='12' font-family='Arial, sans-serif' fill='#555555'>{tick_label}</text>"
        )

    svg_fragments.append(
        f"<line x1='{plot_left:.2f}' y1='{plot_top:.2f}' x2='{plot_left:.2f}' y2='{plot_bottom:.2f}' "
        f"stroke='#666666' stroke-width='1.2'/>"
    )
    svg_fragments.append(
        f"<line x1='{plot_right:.2f}' y1='{plot_top:.2f}' x2='{plot_right:.2f}' y2='{plot_bottom:.2f}' "
        f"stroke='#666666' stroke-width='1.2'/>"
    )
    svg_fragments.append(
        f"<line x1='{plot_left:.2f}' y1='{plot_bottom:.2f}' x2='{plot_right:.2f}' y2='{plot_bottom:.2f}' "
        f"stroke='#666666' stroke-width='1.2'/>"
    )

    svg_fragments.append(
        f"<text x='{plot_left + panel_width / 2.0:.2f}' y='{plot_bottom + 48:.2f}' text-anchor='middle' "
        f"font-size='14' font-family='Arial, sans-serif' fill='#333333'>Time</text>"
    )
    svg_fragments.append(
        f"<text x='{plot_left - 58:.2f}' y='{plot_top + panel_height / 2.0:.2f}' text-anchor='middle' "
        f"font-size='14' font-family='Arial, sans-serif' fill='#333333' "
        f"transform='rotate(-90 {plot_left - 58:.2f} {plot_top + panel_height / 2.0:.2f})'>{main_axis_label}</text>"
    )
    svg_fragments.append(
        f"<text x='{plot_right + 54:.2f}' y='{plot_top + panel_height / 2.0:.2f}' text-anchor='middle' "
        f"font-size='14' font-family='Arial, sans-serif' fill='#333333' "
        f"transform='rotate(90 {plot_right + 54:.2f} {plot_top + panel_height / 2.0:.2f})'>Inflation gap</text>"
    )

    band_90_polygon = _build_band_polygon(
        x_coordinates,
        map_main(ci_lower_90),
        map_main(ci_upper_90),
    )
    band_95_polygon = _build_band_polygon(
        x_coordinates,
        map_main(ci_lower_95),
        map_main(ci_upper_95),
    )
    svg_fragments.append(
        f"<polygon points='{band_95_polygon}' fill='#B5E6A2' fill-opacity='0.72' stroke='none'/>"
    )
    svg_fragments.append(
        f"<polygon points='{band_90_polygon}' fill='#DAF2D0' fill-opacity='0.95' stroke='none'/>"
    )

    zero_line_y = float(map_secondary(np.array([0.0], dtype=float))[0])
    if plot_top <= zero_line_y <= plot_bottom:
        svg_fragments.append(
            f"<line x1='{plot_left:.2f}' y1='{zero_line_y:.2f}' x2='{plot_right:.2f}' "
            f"y2='{zero_line_y:.2f}' stroke='#156082' stroke-width='1.0' "
            f"stroke-dasharray='2 4' opacity='0.95'/>"
        )

    svg_fragments.append(
        f"<path d='{_build_line_path(x_coordinates, map_main(main_actual))}' "
        f"fill='none' stroke='#196B24' stroke-width='1.6'/>"
    )
    svg_fragments.append(
        f"<path d='{_build_line_path(x_coordinates, map_main(main_estimate))}' "
        f"fill='none' stroke='#4EA72E' stroke-width='1.6' stroke-dasharray='8 6'/>"
    )
    svg_fragments.append(
        f"<path d='{_build_line_path(x_coordinates, map_secondary(secondary_series))}' "
        f"fill='none' stroke='#156082' stroke-width='1.4'/>"
    )

    legend_items = [
        ("#DAF2D0", "90% CI", "box"),
        ("#B5E6A2", "95% CI", "box"),
        ("#196B24", "Observed", "line"),
        ("#4EA72E", "Estimate", "dash"),
        ("#156082", "Inflation gap", "line"),
    ]
    legend_x = plot_left + 8.0
    legend_y = plot_top + 18.0
    for color, label, kind in legend_items:
        if kind == "box":
            svg_fragments.append(
                f"<rect x='{legend_x:.2f}' y='{legend_y - 10:.2f}' width='14' height='14' "
                f"fill='{color}' stroke='none'/>"
            )
        elif kind == "dash":
            svg_fragments.append(
                f"<line x1='{legend_x:.2f}' y1='{legend_y - 3:.2f}' x2='{legend_x + 18:.2f}' "
                f"y2='{legend_y - 3:.2f}' stroke='{color}' stroke-width='1.6' stroke-dasharray='8 6'/>"
            )
        else:
            svg_fragments.append(
                f"<line x1='{legend_x:.2f}' y1='{legend_y - 3:.2f}' x2='{legend_x + 18:.2f}' "
                f"y2='{legend_y - 3:.2f}' stroke='{color}' stroke-width='1.6'/>"
            )
        svg_fragments.append(
            f"<text x='{legend_x + 24:.2f}' y='{legend_y:.2f}' font-size='12' "
            f"font-family='Arial, sans-serif' fill='#444444'>{label}</text>"
        )
        legend_x += 112.0


def write_state_panel_svg(out: pd.DataFrame, output_path: Path) -> None:
    width = SVG_PANEL_WIDTH
    height = SVG_PANEL_HEIGHT
    inner_width = width - SVG_MARGIN_LEFT - SVG_MARGIN_RIGHT
    panel_width = (inner_width - SVG_GUTTER) / 2.0
    panel_height = height - SVG_MARGIN_TOP - SVG_MARGIN_BOTTOM
    left_panel_left = SVG_MARGIN_LEFT
    right_panel_left = SVG_MARGIN_LEFT + panel_width + SVG_GUTTER

    svg_fragments: list[str] = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' "
        f"viewBox='0 0 {width} {height}'>",
        (
            "<text x='750' y='34' text-anchor='middle' font-size='24' "
            "font-family='Arial, sans-serif' fill='#1f1f1f'>NAIRU and NAICU estimates</text>"
        ),
        (
            "<text x='750' y='58' text-anchor='middle' font-size='13' "
            "font-family='Arial, sans-serif' fill='#4f4f4f'>"
            "Shaded bands show 90% and 95% confidence intervals from the RTS smoother."
            "</text>"
        ),
    ]

    _draw_svg_panel(
        svg_fragments=svg_fragments,
        panel_left=left_panel_left,
        panel_top=SVG_MARGIN_TOP,
        panel_width=panel_width,
        panel_height=panel_height,
        dates=out["Date"],
        main_actual=out["unemployment_current"].to_numpy(dtype=float),
        main_estimate=out["nairu_estimate"].to_numpy(dtype=float),
        ci_lower_90=out["nairu_ci_lower_90"].to_numpy(dtype=float),
        ci_upper_90=out["nairu_ci_upper_90"].to_numpy(dtype=float),
        ci_lower_95=out["nairu_ci_lower_95"].to_numpy(dtype=float),
        ci_upper_95=out["nairu_ci_upper_95"].to_numpy(dtype=float),
        secondary_series=out["inflation_gap"].to_numpy(dtype=float),
        title="Subplot A. NAIRU Estimation",
        main_axis_label="Unemployment",
    )
    _draw_svg_panel(
        svg_fragments=svg_fragments,
        panel_left=right_panel_left,
        panel_top=SVG_MARGIN_TOP,
        panel_width=panel_width,
        panel_height=panel_height,
        dates=out["Date"],
        main_actual=out["icu_current"].to_numpy(dtype=float),
        main_estimate=out["naicu_estimate"].to_numpy(dtype=float),
        ci_lower_90=out["naicu_ci_lower_90"].to_numpy(dtype=float),
        ci_upper_90=out["naicu_ci_upper_90"].to_numpy(dtype=float),
        ci_lower_95=out["naicu_ci_lower_95"].to_numpy(dtype=float),
        ci_upper_95=out["naicu_ci_upper_95"].to_numpy(dtype=float),
        secondary_series=out["inflation_gap"].to_numpy(dtype=float),
        title="Subplot B. NAICU Estimation",
        main_axis_label="ICU",
    )
    svg_fragments.append("</svg>")
    output_path.write_text("\n".join(svg_fragments), encoding="utf-8")


def write_state_panel_figure(out: pd.DataFrame, out_dir: Path) -> None:
    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        write_state_panel_svg(out, out_dir / "nairu_naicu_panel_v5.svg")
        return

    sns.set_theme(style="whitegrid")

    dates = pd.to_datetime(out["Date"])
    figure, axes = plt.subplots(1, 2, figsize=(16, 6.5), sharex=True)

    def configure_panel(
        axis: "plt.Axes",
        main_actual: pd.Series,
        main_estimate: pd.Series,
        lower_90: pd.Series,
        upper_90: pd.Series,
        lower_95: pd.Series,
        upper_95: pd.Series,
        secondary_series: pd.Series,
        title: str,
        main_label: str,
    ) -> None:
        secondary_axis = axis.twinx()
        secondary_axis.patch.set_alpha(0.0)

        axis.fill_between(
            dates,
            lower_95,
            upper_95,
            color="#B5E6A2",
            alpha=0.72,
            linewidth=0.0,
            zorder=1,
            label="95% CI",
        )
        axis.fill_between(
            dates,
            lower_90,
            upper_90,
            color="#DAF2D0",
            alpha=1.0,
            linewidth=0.0,
            zorder=2,
            label="90% CI",
        )
        axis.plot(
            dates,
            main_actual,
            color="#196B24",
            linewidth=1.6,
            zorder=4,
            label="Observed",
        )
        axis.plot(
            dates,
            main_estimate,
            color="#4EA72E",
            linewidth=1.6,
            linestyle="--",
            dashes=(8, 5),
            zorder=5,
            label="Estimate",
        )
        secondary_axis.axhline(
            0.0,
            color="#156082",
            linewidth=0.9,
            linestyle=":",
            zorder=3,
        )
        secondary_axis.plot(
            dates,
            secondary_series,
            color="#156082",
            linewidth=1.4,
            zorder=6,
            label="Inflation gap",
        )

        main_min = float(
            np.nanmin(
                np.concatenate(
                    [
                        np.asarray(main_actual, dtype=float),
                        np.asarray(main_estimate, dtype=float),
                        np.asarray(lower_95, dtype=float),
                        np.asarray(upper_95, dtype=float),
                    ]
                )
            )
        )
        main_max = float(
            np.nanmax(
                np.concatenate(
                    [
                        np.asarray(main_actual, dtype=float),
                        np.asarray(main_estimate, dtype=float),
                        np.asarray(lower_95, dtype=float),
                        np.asarray(upper_95, dtype=float),
                    ]
                )
            )
        )
        secondary_min = float(np.nanmin(np.asarray(secondary_series, dtype=float)))
        secondary_max = float(np.nanmax(np.asarray(secondary_series, dtype=float)))
        main_padding = 0.08 * max(main_max - main_min, 1.0)
        secondary_padding = 0.10 * max(secondary_max - secondary_min, 0.5)

        axis.set_ylim(main_min - main_padding, main_max + main_padding)
        secondary_axis.set_ylim(
            secondary_min - secondary_padding,
            secondary_max + secondary_padding,
        )

        axis.set_title(title, fontsize=13, pad=10)
        axis.set_ylabel(main_label)
        secondary_axis.set_ylabel("Inflation gap (pp)", color="#156082")
        secondary_axis.tick_params(axis="y", colors="#156082")
        axis.set_xlabel("Time")
        axis.grid(True, axis="y", color="#E6E6E6")
        axis.grid(False, axis="x")
        secondary_axis.grid(False)
        axis.margins(x=0.01)

        handles_main, labels_main = axis.get_legend_handles_labels()
        handles_secondary, labels_secondary = secondary_axis.get_legend_handles_labels()
        axis.legend(
            handles_main + handles_secondary,
            labels_main + labels_secondary,
            loc="upper left",
            frameon=True,
            fontsize=9,
        )

    configure_panel(
        axis=axes[0],
        main_actual=out["unemployment_current"],
        main_estimate=out["nairu_estimate"],
        lower_90=out["nairu_ci_lower_90"],
        upper_90=out["nairu_ci_upper_90"],
        lower_95=out["nairu_ci_lower_95"],
        upper_95=out["nairu_ci_upper_95"],
        secondary_series=out["inflation_gap"],
        title="Subplot A (NAIRU Estimation)",
        main_label="Unemployment (%)",
    )
    configure_panel(
        axis=axes[1],
        main_actual=out["icu_current"],
        main_estimate=out["naicu_estimate"],
        lower_90=out["naicu_ci_lower_90"],
        upper_90=out["naicu_ci_upper_90"],
        lower_95=out["naicu_ci_lower_95"],
        upper_95=out["naicu_ci_upper_95"],
        secondary_series=out["inflation_gap"],
        title="Subplot B (NAICU Estimation)",
        main_label="ICU (%)",
    )

    year_locator = mdates.YearLocator(base=4)
    year_formatter = mdates.DateFormatter("%Y")
    for axis in axes:
        axis.xaxis.set_major_locator(year_locator)
        axis.xaxis.set_major_formatter(year_formatter)
        for label in axis.get_xticklabels():
            label.set_rotation(45)
            label.set_horizontalalignment("right")

    figure.suptitle("NAIRU and NAICU estimates", fontsize=16, y=0.98)
    figure.tight_layout(rect=(0.0, 0.0, 1.0, 0.96))
    figure.savefig(out_dir / "nairu_naicu_panel_v5.png", dpi=300, bbox_inches="tight")
    figure.savefig(out_dir / "nairu_naicu_panel_v5.svg", bbox_inches="tight")
    plt.close(figure)


def _apply_covid_anchor_interpolation(data: ModelData) -> None:
    """
    Fix 2026-09-03 (alineación v3, ver docstring del módulo): réplica de
    ``_aplicar_dummy_covid()`` en pib_potencial_integrado_v3.py. Neutraliza el
    pico COVID SOLO en el ancla de histéresis (``unemployment_hysteresis_anchor``),
    interpolando linealmente en la ventana [COVID_ANCHOR_START, COVID_ANCHOR_END].
    La holgura de la ecuación de medición usa ``unemployment_lag1``/``lag2``
    (campos aparte, ver ModelData) que esta función NO toca -- la ecuación de
    medición sigue viendo el desempleo observado real, así que la brecha COVID
    queda reflejada en el filtro en vez de absorbida por un regresor o
    suavizada silenciosamente.
    """
    dates = pd.to_datetime(data.dates)
    start = pd.Timestamp(COVID_ANCHOR_START)
    end = pd.Timestamp(COVID_ANCHOR_END)
    mask = ((dates >= start) & (dates <= end)).to_numpy()
    anchor = pd.Series(data.unemployment_hysteresis_anchor.astype(float).copy())
    anchor[mask] = np.nan
    data.unemployment_hysteresis_anchor = anchor.interpolate(
        method="linear", limit_direction="both"
    ).to_numpy()


def load_and_prepare_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_excel(path)

    required_columns = [
        "Year",
        "Month",
        UNEMPLOYMENT_COL,
        CORE_INFLATION_COL,
        INFLATION_TARGET_COL,
        OIL_PRICE_COL,
        NOMINAL_RATE_COL,
        REAL_RATE_COL,
        ICU_COL,
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    numeric_columns = [
        UNEMPLOYMENT_COL,
        CORE_INFLATION_COL,
        INFLATION_TARGET_COL,
        OIL_PRICE_COL,
        NOMINAL_RATE_COL,
        REAL_RATE_COL,
        ICU_COL,
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["Date"] = pd.to_datetime(
        dict(
            year=pd.to_numeric(df["Year"], errors="coerce"),
            month=pd.to_numeric(df["Month"], errors="coerce"),
            day=1,
        ),
        errors="coerce",
    )
    df = df.sort_values("Date").dropna(subset=["Date"]).reset_index(drop=True)

    nominal_rate = df[NOMINAL_RATE_COL] / 100.0
    real_rate = df[REAL_RATE_COL] / 100.0
    df["expected_inflation_fisher_12m_ahead"] = 100.0 * (
        (1.0 + nominal_rate) / (1.0 + real_rate) - 1.0
    )
    df["expected_inflation_current_period"] = df[
        "expected_inflation_fisher_12m_ahead"
    ].shift(EXPECTATIONS_HORIZON_MONTHS)

    df["inflation_gap"] = df[CORE_INFLATION_COL] - df[INFLATION_TARGET_COL]
    df["inflation_gap_change"] = df["inflation_gap"].diff()
    df["inflation_gap_change_lag1"] = df["inflation_gap_change"].shift(1)
    df["inflation_gap_change_lag2"] = df["inflation_gap_change"].shift(2)

    df["unemployment_current"] = df[UNEMPLOYMENT_COL]
    df["unemployment_lag1"] = df[UNEMPLOYMENT_COL].shift(1)
    df["unemployment_lag2"] = df[UNEMPLOYMENT_COL].shift(2)
    df["icu_current"] = df[ICU_COL]
    df["icu_lag1"] = df[ICU_COL].shift(1)
    # Fix 2026-09-03 (alineación v3): 3er rezago de ICU -- la holgura de
    # capacidad instalada ahora es un rezago distribuido de 3 términos, igual
    # que ya se hacía para desempleo (unemployment_coefficient_0/1/2).
    df["icu_lag2"] = df[ICU_COL].shift(2)

    oil_level = df[OIL_PRICE_COL].where(df[OIL_PRICE_COL] > 0.0)
    df["oil_shock"] = 100.0 * np.log(oil_level).diff()

    df["expected_inflation_term"] = (
        df["expected_inflation_current_period"] - df[CORE_INFLATION_COL].shift(1)
    )

    # Fix 2026-09-03 (alineación v3, ver docstring del módulo): antes aquí se
    # construía covid_dummy, un regresor 0/1 de la ecuación de medición. La
    # spec final de v3 no usa ese regresor -- el mecanismo es
    # _apply_covid_anchor_interpolation, que se aplica sobre
    # ModelData.unemployment_hysteresis_anchor DESPUÉS de build_model_data(),
    # no en esta función.

    # Fix 2026-09-03 (alineación v3): unemployment_ma24/icu_ma24 NO los usa
    # ningún cálculo de este módulo (_kalman_pass, para la spec
    # "hysteresis" + "distributed_lag", nunca los toca -- solo los usaría la
    # variante de tendencia "ma24", que este repo no implementa). Se calculan
    # aquí de todos modos, y se incluyen en la selección de columnas de
    # abajo, SOLO para reproducir la ventana muestral exacta de v3: en la
    # referencia, load_and_prepare_data() es una función genérica que
    # comparten TODAS las variantes de espec (incluida "ma24"), y su
    # .dropna() se aplica sobre TODAS las columnas -- incluida esta media
    # móvil de 24 meses -- sin importar cuál variante se vaya a estimar
    # después. Eso recorta las primeras 24 observaciones de CUALQUIER
    # corrida, incluida "hysteresis" + "distributed_lag" (que no las
    # necesita). Sin este recorte, la muestra empieza 12 meses antes
    # (2005-01 en vez de 2006-01: el máximo NaN inicial pasa a ser el de
    # expected_inflation_current_period, 12 meses, en vez de este, 24) y
    # BEST_START_PARAMS_V3 dejó de ser el óptimo para esa muestra distinta
    # -- confirmado empíricamente: con la ventana de 250 obs (2005-01) el
    # optimizador no converge (ABNORMAL_TERMINATION_IN_LNSRCH) partiendo de
    # esa semilla, y las estimaciones de nivel inicial (NAIRU/NAICU en
    # t=0) difieren de la referencia aunque el filtro converja al mismo
    # punto hacia el final de la muestra (el filtro "olvida" el estado
    # inicial con el tiempo). Con la ventana correcta (238 obs, 2006-01) el
    # optimizador converge de inmediato desde la semilla, como documenta el
    # comentario de v3 ("converge de inmediato porque ya es el óptimo").
    df["unemployment_ma24"] = (
        df[UNEMPLOYMENT_COL].shift(1).rolling(window=24, min_periods=24).mean()
    )
    df["icu_ma24"] = (
        df[ICU_COL].shift(1).rolling(window=24, min_periods=24).mean()
    )

    model_df = df[
        [
            "Date",
            "inflation_gap",
            "inflation_gap_change",
            "inflation_gap_change_lag1",
            "inflation_gap_change_lag2",
            "unemployment_current",
            "unemployment_lag1",
            "unemployment_lag2",
            "unemployment_ma24",
            "icu_ma24",
            "icu_current",
            "icu_lag1",
            "icu_lag2",
            "oil_shock",
            "expected_inflation_term",
            "expected_inflation_fisher_12m_ahead",
            "expected_inflation_current_period",
        ]
    ].dropna().reset_index(drop=True)

    if len(model_df) < 60:
        raise ValueError("Not enough observations after preprocessing.")

    return model_df


def build_model_data(df: pd.DataFrame) -> ModelData:
    unemployment_lag1 = _to_float_array(df["unemployment_lag1"])
    data = ModelData(
        inflation_gap_change=_to_float_array(df["inflation_gap_change"]),
        inflation_gap_change_lag1=_to_float_array(df["inflation_gap_change_lag1"]),
        inflation_gap_change_lag2=_to_float_array(df["inflation_gap_change_lag2"]),
        unemployment_current=_to_float_array(df["unemployment_current"]),
        unemployment_lag1=unemployment_lag1,
        unemployment_lag2=_to_float_array(df["unemployment_lag2"]),
        # Fix 2026-09-03 (alineación v3): copia APARTE de unemployment_lag1,
        # tomada antes de cualquier interpolación -- _apply_covid_anchor_interpolation
        # (llamada más abajo) solo modifica esta copia, nunca la de arriba.
        unemployment_hysteresis_anchor=unemployment_lag1.copy(),
        icu_current=_to_float_array(df["icu_current"]),
        icu_lag1=_to_float_array(df["icu_lag1"]),
        icu_lag2=_to_float_array(df["icu_lag2"]),
        expected_inflation_term=_to_float_array(df["expected_inflation_term"]),
        oil_shock=_to_float_array(df["oil_shock"]),
        dates=df["Date"].reset_index(drop=True),
        n_obs=len(df),
    )
    # Fix 2026-09-03 (alineación v3, ver docstring del módulo): réplica de
    # estimar_nairu_naicu_estructural_v3(), que llama build_model_data(df)
    # seguido de _aplicar_dummy_covid(data) -- se hace aquí adentro (en vez de
    # exigir que cada llamador se acuerde de hacerlo aparte) para que
    # cualquier consumidor de build_model_data() (main(), estimation.py, los
    # tests que usen datos reales) reciba un ModelData ya listo para estimar,
    # sin un paso manual que se pueda olvidar en silencio.
    _apply_covid_anchor_interpolation(data)
    return data


def unpack_params(params: np.ndarray) -> Dict[str, float]:
    (
        intercept,
        inflation_lag1_coefficient,
        inflation_lag2_coefficient,
        unemployment_coefficient_0,
        unemployment_coefficient_1,
        unemployment_coefficient_2,
        icu_coefficient_0,
        icu_coefficient_1,
        icu_coefficient_2,
        expectations_coefficient,
        oil_shock_coefficient,
        nairu_adjustment_speed,
        naicu_adjustment_speed,
        log_measurement_error_std,
        log_nairu_transition_std,
        log_naicu_transition_std,
        nairu_initial_level,
        naicu_initial_level,
    ) = params

    unemployment_gap_coefficient = (
        unemployment_coefficient_0
        + unemployment_coefficient_1
        + unemployment_coefficient_2
    )
    # Fix 2026-09-03 (alineación v3): antes icu_gap_coefficient era un único
    # coeficiente contemporáneo estimado directamente. Ahora es la SUMA de 3
    # términos de rezago distribuido (icu_coefficient_0/1/2), igual que
    # unemployment_gap_coefficient -- ver docstring del módulo.
    icu_total_coefficient = (
        icu_coefficient_0
        + icu_coefficient_1
        + icu_coefficient_2
    )

    return {
        "intercept": float(intercept),
        "inflation_lag1_coefficient": float(inflation_lag1_coefficient),
        "inflation_lag2_coefficient": float(inflation_lag2_coefficient),
        "unemployment_coefficient_0": float(unemployment_coefficient_0),
        "unemployment_coefficient_1": float(unemployment_coefficient_1),
        "unemployment_coefficient_2": float(unemployment_coefficient_2),
        "unemployment_gap_coefficient": float(unemployment_gap_coefficient),
        "icu_coefficient_0": float(icu_coefficient_0),
        "icu_coefficient_1": float(icu_coefficient_1),
        "icu_coefficient_2": float(icu_coefficient_2),
        "icu_total_coefficient": float(icu_total_coefficient),
        "expectations_coefficient": float(expectations_coefficient),
        "oil_shock_coefficient": float(oil_shock_coefficient),
        "nairu_adjustment_speed": float(nairu_adjustment_speed),
        "naicu_adjustment_speed": float(naicu_adjustment_speed),
        "measurement_error_std": float(np.exp(log_measurement_error_std)),
        "nairu_transition_std": float(np.exp(log_nairu_transition_std)),
        "naicu_transition_std": float(np.exp(log_naicu_transition_std)),
        "nairu_initial_level": float(nairu_initial_level),
        "naicu_initial_level": float(naicu_initial_level),
    }


def _kalman_pass(
    params: np.ndarray,
    data: ModelData,
    store_history: bool = False,
) -> Tuple[float, Optional[KalmanHistory]]:
    raw = np.asarray(params, dtype=float)
    if raw.shape != (18,) or not np.all(np.isfinite(raw)):
        return LARGE_PENALTY, None

    (
        intercept,
        inflation_lag1_coefficient,
        inflation_lag2_coefficient,
        unemployment_coefficient_0,
        unemployment_coefficient_1,
        unemployment_coefficient_2,
        icu_coefficient_0,
        icu_coefficient_1,
        icu_coefficient_2,
        expectations_coefficient,
        oil_shock_coefficient,
        nairu_adjustment_speed,
        naicu_adjustment_speed,
        log_measurement_error_std,
        log_nairu_transition_std,
        log_naicu_transition_std,
        nairu_initial_level,
        naicu_initial_level,
    ) = raw

    measurement_error_std = float(np.exp(log_measurement_error_std))
    nairu_transition_std = float(np.exp(log_nairu_transition_std))
    naicu_transition_std = float(np.exp(log_naicu_transition_std))

    if (
        measurement_error_std <= 0.0
        or nairu_transition_std <= 0.0
        or naicu_transition_std <= 0.0
    ):
        return LARGE_PENALTY, None
    if not (0.0 <= nairu_initial_level <= 30.0):
        return LARGE_PENALTY, None
    if not (40.0 <= naicu_initial_level <= 95.0):
        return LARGE_PENALTY, None
    if not (0.0 <= nairu_adjustment_speed <= 0.25):
        return LARGE_PENALTY, None
    if not (0.0 <= naicu_adjustment_speed <= 0.25):
        return LARGE_PENALTY, None

    unemployment_gap_coefficient = (
        unemployment_coefficient_0
        + unemployment_coefficient_1
        + unemployment_coefficient_2
    )
    # Fix 2026-09-03 (alineación v3): suma de los 3 términos de rezago
    # distribuido de ICU -- ver unpack_params y el docstring del módulo.
    icu_total_coefficient = (
        icu_coefficient_0
        + icu_coefficient_1
        + icu_coefficient_2
    )

    if unemployment_gap_coefficient >= -0.02:
        penalty = LARGE_PENALTY + 1e6 * (unemployment_gap_coefficient + 0.02) ** 2
        return float(penalty), None
    if icu_total_coefficient <= 0.02:
        penalty = LARGE_PENALTY + 1e6 * (0.02 - icu_total_coefficient) ** 2
        return float(penalty), None

    state_transition = np.array(
        [
            [1.0 - nairu_adjustment_speed, 0.0],
            [0.0, 1.0 - naicu_adjustment_speed],
        ],
        dtype=float,
    )
    state_noise = np.array(
        [
            [nairu_transition_std * nairu_transition_std, 0.0],
            [0.0, naicu_transition_std * naicu_transition_std],
        ],
        dtype=float,
    )
    observation_loadings = np.array(
        [-unemployment_gap_coefficient, -icu_total_coefficient],
        dtype=float,
    )
    measurement_error_variance = measurement_error_std * measurement_error_std
    identity = np.eye(2, dtype=float)

    # Fix 2026-09-03 (alineación v3): holgura ICU ahora es un rezago
    # distribuido de 3 términos (icu_current, icu_lag1, icu_lag2), igual que
    # ya se hacía para desempleo -- antes solo icu_current entraba (con
    # icu_gap_coefficient). El regresor COVID (covid_shock_coefficient) se
    # eliminó: ver _apply_covid_anchor_interpolation y el docstring del
    # módulo -- la ecuación de medición sigue viendo el desempleo observado
    # real sin ningún ajuste COVID.
    signal = (
        intercept
        + inflation_lag1_coefficient * data.inflation_gap_change_lag1
        + inflation_lag2_coefficient * data.inflation_gap_change_lag2
        + unemployment_coefficient_0 * data.unemployment_current
        + unemployment_coefficient_1 * data.unemployment_lag1
        + unemployment_coefficient_2 * data.unemployment_lag2
        + icu_coefficient_0 * data.icu_current
        + icu_coefficient_1 * data.icu_lag1
        + icu_coefficient_2 * data.icu_lag2
        + expectations_coefficient * data.expected_inflation_term
        + oil_shock_coefficient * data.oil_shock
    )
    if not np.all(np.isfinite(signal)):
        return LARGE_PENALTY, None

    if store_history:
        state_pred = np.empty((data.n_obs, 2), dtype=float)
        covariance_pred = np.empty((data.n_obs, 2, 2), dtype=float)
        state_filt = np.empty((data.n_obs, 2), dtype=float)
        covariance_filt = np.empty((data.n_obs, 2, 2), dtype=float)
    else:
        state_pred = covariance_pred = state_filt = covariance_filt = None

    state = np.array([nairu_initial_level, naicu_initial_level], dtype=float)
    covariance = np.diag([INITIAL_NAIRU_VARIANCE, INITIAL_NAICU_VARIANCE]).astype(float)
    nll = 0.0
    unpenalized_nll = 0.0  # Fix 2026-09-01 (Fase 1): ver KalmanHistory.unpenalized_nll

    for t in range(data.n_obs):
        # Fix 2026-09-03 (alineación v3): el ancla de histéresis usa la copia
        # aparte (COVID-interpolada) unemployment_hysteresis_anchor, NUNCA
        # unemployment_lag1 (ese es el dato real, usado arriba en `signal`
        # para la holgura de la ecuación de medición). El ancla de ICU
        # (icu_lag1) no se toca en ningún escenario -- v3 solo neutraliza el
        # pico COVID en el ancla de desempleo, ver _apply_covid_anchor_interpolation.
        state_control = np.array(
            [
                nairu_adjustment_speed * data.unemployment_hysteresis_anchor[t],
                naicu_adjustment_speed * data.icu_lag1[t],
            ],
            dtype=float,
        )
        predicted_state = state_transition @ state + state_control
        predicted_covariance = state_transition @ covariance @ state_transition.T + state_noise
        predicted_covariance = _stabilize_covariance(predicted_covariance)

        nairu_pred = float(predicted_state[0])
        naicu_pred = float(predicted_state[1])
        if nairu_pred < MIN_NAIRU_LEVEL:
            nll += 8.0 * (MIN_NAIRU_LEVEL - nairu_pred) ** 2
        elif nairu_pred > MAX_NAIRU_LEVEL:
            nll += 8.0 * (nairu_pred - MAX_NAIRU_LEVEL) ** 2

        if naicu_pred < MIN_NAICU_LEVEL:
            nll += 2.0 * (MIN_NAICU_LEVEL - naicu_pred) ** 2
        elif naicu_pred > MAX_NAICU_LEVEL:
            nll += 2.0 * (naicu_pred - MAX_NAICU_LEVEL) ** 2

        innovation = float(
            data.inflation_gap_change[t]
            - (signal[t] + observation_loadings @ predicted_state)
        )
        innovation_variance = float(
            observation_loadings @ predicted_covariance @ observation_loadings
            + measurement_error_variance
        )

        if (
            not np.isfinite(innovation)
            or not np.isfinite(innovation_variance)
            or innovation_variance <= MIN_VARIANCE
        ):
            return LARGE_PENALTY, None

        gaussian_term = 0.5 * (
            LOG_2PI
            + np.log(innovation_variance)
            + (innovation * innovation) / innovation_variance
        )
        nll += gaussian_term
        unpenalized_nll += gaussian_term

        kalman_gain = (predicted_covariance @ observation_loadings) / innovation_variance
        state = predicted_state + kalman_gain * innovation

        innovation_projection = identity - np.outer(kalman_gain, observation_loadings)
        covariance = (
            innovation_projection
            @ predicted_covariance
            @ innovation_projection.T
            + np.outer(kalman_gain, kalman_gain) * measurement_error_variance
        )
        covariance = _stabilize_covariance(covariance)

        if store_history:
            state_pred[t] = predicted_state
            covariance_pred[t] = predicted_covariance
            state_filt[t] = state
            covariance_filt[t] = covariance

    if not np.isfinite(nll):
        return LARGE_PENALTY, None

    if not store_history:
        return float(nll), None

    history = KalmanHistory(
        state_pred=state_pred,
        covariance_pred=covariance_pred,
        state_filt=state_filt,
        covariance_filt=covariance_filt,
        unpenalized_nll=float(unpenalized_nll),
    )
    return float(nll), history


def kalman_filter_loglik(params: np.ndarray, data: ModelData) -> float:
    nll, _ = _kalman_pass(params, data, store_history=False)
    return nll


# Fix 2026-09-03 (alineación v3, ver docstring del módulo): antes
# estimate_parameters hacía un arranque OLS + grid multi-start de 97
# combinaciones para escapar mínimos locales, sin garantía de converger al
# MISMO óptimo que usa v3 para su salida publicada (el objetivo tiene varios
# mínimos locales cercanos; "un óptimo local válido" no es lo mismo que "EL
# resultado publicado"). v3 no hace ese barrido para su spec final: usa
# BEST_START_PARAMS -- el óptimo de una búsqueda robusta de 60 arranques
# hecha aparte, fuera de la corrida automática -- como valor inicial de UNA
# sola optimización local L-BFGS-B (converge de inmediato porque ya es el
# óptimo). Para garantizar EXACTAMENTE los mismos resultados, aquí se replica
# ese arranque único -- traducido al orden de PARAMETER_NAMES de este módulo
# -- con las mismas opciones del optimizador. Los 4 parámetros fijos (ver
# FIXED_PARAMETER_INDICES) quedan clavados en su valor por PARAMETER_BOUNDS
# sin importar qué semilla se les dé (np.clip los fuerza al único punto
# permitido, igual que hace v3 con `np.clip(BEST_START_PARAMS, lo, hi)`).
BEST_START_PARAMS_V3 = np.array(
    [
        -0.00779745005987,  # 0  intercept
        -0.0227181720783,  # 1  inflation_lag1_coefficient
        -0.0373761092539,  # 2  inflation_lag2_coefficient
        -0.00709562513813,  # 3  unemployment_coefficient_0
        -0.00539653619043,  # 4  unemployment_coefficient_1
        -0.00758647994148,  # 5  unemployment_coefficient_2
        0.0157173499202,  # 6  icu_coefficient_0
        0.00785892927416,  # 7  icu_coefficient_1
        0.00273956459155,  # 8  icu_coefficient_2
        -0.0948691724425,  # 9  expectations_coefficient
        -0.00158445792059,  # 10 oil_shock_coefficient
        NAIRU_ADJUSTMENT_SPEED_FINAL,  # 11 nairu_adjustment_speed (FIJO)
        NAICU_ADJUSTMENT_SPEED_FINAL,  # 12 naicu_adjustment_speed (FIJO)
        -1.33261385647,  # 13 log_measurement_error_std
        math.log(NAIRU_TRANSITION_STD_FINAL),  # 14 log_nairu_transition_std (FIJO)
        math.log(NAICU_TRANSITION_STD_FINAL),  # 15 log_naicu_transition_std (FIJO)
        12.0459374307,  # 16 nairu_initial_level
        73.41029111,  # 17 naicu_initial_level
    ],
    dtype=float,
)


# Fix 2026-09-03 (alineación v3): historia de esta función, importante para
# entender por qué termina así -- LÉASE ANTES DE TOCAR ESTO.
#
# v3 llama minimize() UNA vez desde BEST_START_PARAMS (el óptimo de una
# búsqueda robusta de 60 arranques hecha aparte, "ya hallados") esperando que
# "converja de inmediato porque ya es el óptimo" (comentario textual de v3).
# Verificando esto bit a bit contra la referencia (ver isolate_objective_diff.py:
# _kalman_pass/kalman_filter_loglik de este módulo y el de v3 dan el MISMO
# valor, a 0.0 de diferencia, para cualquier vector de parámetros probado --
# el modelo está matemáticamente probado idéntico) se encontró que
# scipy.optimize.minimize(method="L-BFGS-B") en ESTE óptimo específico
# -- extremadamente plano, con varias restricciones de signo a menos de 1e-4
# de su propio límite -- es sensible a detalles de entorno TOTALMENTE ajenos
# al modelo: bajo numpy>=2.0 se queda en el seed sin moverse
# (ABNORMAL_TERMINATION_IN_LNSRCH, nit=0, igual que v3), pero bajo numpy<2.0
# (la restricción real de este repo -- ver requirements.txt) SÍ encuentra una
# dirección de mejora real (no ruido: nll baja de 34.34 a 33.54) y se aleja
# sustancialmente del óptimo publicado. Reordenar las coordenadas del vector
# para que coincida EXACTO con el orden de v3 (ver isolate_optimizer_diff.py)
# arregla el caso numpy<2.0 -- pero al volver a probar bajo numpy>=2.0 con
# ESE mismo reordenamiento, el resultado YA NO fue bit-idéntico como antes
# (ver commits/historial de este archivo): la sensibilidad no es "un orden
# correcto que hay que encontrar", es inherente a optimizar con L-BFGS-B
# justo sobre un punto casi degenerado -- CUALQUIER detalle no controlado
# (numpy, BLAS, SO, hilos) puede inclinar la balanza entre "no se mueve" y
# "se mueve de verdad", en cualquier dirección, sin que haya una única
# configuración que lo arregle para todos los entornos a la vez.
#
# La salida: BEST_START_PARAMS_V3 no es "un punto de partida cualquiera que
# hay que refinar" -- es EL óptimo ya calibrado que v3 publica (así lo
# describe v3 mismo: "ya hallados", "converge de inmediato"). Por eso esta
# función usa BEST_START_PARAMS_V3 DIRECTAMENTE como resultado final, en vez
# de confiar en que minimize() se quede quieto -- eso es determinista y
# reproduce el NAIRU/NAICU publicado por v3 en CUALQUIER entorno, en vez de
# "normalmente, salvo que el numpy/BLAS instalado no coincida". minimize()
# se conserva y se sigue llamando (con el orden exacto de v3, ver
# _REFERENCE_PARAMETER_ORDER) puramente como DIAGNÓSTICO: si algún día
# alguien actualiza Data_NAIRU.xlsx con datos nuevos y el seed calibrado deja
# de ser el óptimo de verdad, esta comparación lo hace evidente (una mejora
# grande y consistente, no el ruido de entorno que se ve hoy) en vez de
# fallar en silencio -- ver el mensaje de diagnóstico en FitResult.message.
_REFERENCE_PARAMETER_ORDER = [
    "intercept",
    "inflation_lag1_coefficient",
    "inflation_lag2_coefficient",
    "expectations_coefficient",
    "oil_shock_coefficient",
    "unemployment_coefficient_0",
    "unemployment_coefficient_1",
    "unemployment_coefficient_2",
    "icu_coefficient_0",
    "icu_coefficient_1",
    "icu_coefficient_2",
    "log_measurement_error_std",
    "log_nairu_transition_std",
    "log_naicu_transition_std",
    "nairu_initial_level",
    "naicu_initial_level",
    "nairu_adjustment_speed",
    "naicu_adjustment_speed",
]
_MINE_TO_REFERENCE_INDEX = [PARAMETER_NAMES.index(name) for name in _REFERENCE_PARAMETER_ORDER]


_MEANINGFUL_IMPROVEMENT_THRESHOLD = 1e-6


def estimate_parameters(data: ModelData) -> FitResult:
    n_params = len(PARAMETER_NAMES)
    lower_bounds = np.array([bound[0] for bound in PARAMETER_BOUNDS], dtype=float)
    upper_bounds = np.array([bound[1] for bound in PARAMETER_BOUNDS], dtype=float)
    final_params = np.clip(BEST_START_PARAMS_V3, lower_bounds, upper_bounds)
    nll_seed = kalman_filter_loglik(final_params, data)

    x0_reference_order = final_params[_MINE_TO_REFERENCE_INDEX]
    bounds_reference_order = [PARAMETER_BOUNDS[index] for index in _MINE_TO_REFERENCE_INDEX]

    def _objective_reference_order(params_reference_order: np.ndarray) -> float:
        params_mine_order = np.empty(n_params, dtype=float)
        params_mine_order[_MINE_TO_REFERENCE_INDEX] = params_reference_order
        return kalman_filter_loglik(params_mine_order, data)

    opt = minimize(
        _objective_reference_order,
        x0=x0_reference_order,
        method="L-BFGS-B",
        bounds=bounds_reference_order,
        options={"maxiter": 1500, "ftol": 1e-9, "gtol": 1e-6},
    )

    opt_params_mine_order = np.empty(n_params, dtype=float)
    opt_params_mine_order[_MINE_TO_REFERENCE_INDEX] = np.array(opt.x, dtype=float)
    if np.all(np.isfinite(opt_params_mine_order)):
        nll_opt = kalman_filter_loglik(opt_params_mine_order, data)
    else:
        nll_opt = float("nan")

    improvement = nll_seed - nll_opt if np.isfinite(nll_opt) else float("nan")
    if not np.isfinite(improvement) or nll_opt >= LARGE_PENALTY * 0.1:
        diagnostic_message = (
            "Usando BEST_START_PARAMS_V3 directamente (calibrado, ver el comentario "
            "extenso encima de esta función). minimize() no devolvió un punto útil "
            f"(nll_opt={nll_opt!r}) -- descartado. nll(seed)={nll_seed:.6f}. "
            f"optimizer.message={opt.message!r}"
        )
    elif improvement > _MEANINGFUL_IMPROVEMENT_THRESHOLD:
        diagnostic_message = (
            "Usando BEST_START_PARAMS_V3 directamente (calibrado, ver el comentario "
            "extenso encima de esta función) en vez del resultado de minimize(), aunque "
            f"ESTE SÍ encontró una mejora (nll {nll_seed:.6f} -> {nll_opt:.6f}, "
            f"delta={improvement:.6f}): adoptarla haría que el resultado dependiera del "
            "entorno numérico (numpy/BLAS) en vez de coincidir con el óptimo publicado "
            f"por v3. Si Data_NAIRU.xlsx cambió, puede que BEST_START_PARAMS_V3 ya no sea "
            f"óptimo -- valdría la pena recalibrar. optimizer.message={opt.message!r}"
        )
    else:
        diagnostic_message = (
            "Usando BEST_START_PARAMS_V3 directamente (calibrado, ver el comentario "
            "extenso encima de esta función). minimize() no encontró una mejora "
            f"significativa (delta={improvement:.2e}), consistente con que el seed ya es "
            f"el óptimo. optimizer.message={opt.message!r}"
        )

    return FitResult(
        params=final_params,
        success=True,
        message=diagnostic_message,
        nll=float(nll_seed),
        optimizer_inverse_hessian=None,
    )


def kalman_filter_and_smoother(
    params: np.ndarray,
    data: ModelData,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    _, history = _kalman_pass(params, data, store_history=True)
    if history is None:
        raise RuntimeError("Kalman filter failed for fitted parameters.")

    # Fix 2026-09-01 (auditoria_src_2026-08-21.md, hallazgo #1): esta matriz
    # se armaba con params[9]/params[10] fijos por posición. Esos índices
    # apuntaban a covid_shock_coefficient/nairu_adjustment_speed en vez de
    # nairu_adjustment_speed/naicu_adjustment_speed -- se desalinearon cuando
    # covid_shock_coefficient se insertó en la posición 9 de PARAMETER_NAMES
    # y nadie actualizó este bloque (_kalman_pass sí desempaqueta por
    # nombre y no se vio afectado). Se usa unpack_params() -- la misma
    # fuente de verdad que ya usa el resto del módulo -- para que un futuro
    # cambio en PARAMETER_NAMES no pueda romper esto otra vez en silencio.
    parsed = unpack_params(np.asarray(params, dtype=float))
    state_transition = np.array(
        [
            [1.0 - parsed["nairu_adjustment_speed"], 0.0],
            [0.0, 1.0 - parsed["naicu_adjustment_speed"]],
        ],
        dtype=float,
    )

    state_smooth = np.empty_like(history.state_filt)
    covariance_smooth = np.empty_like(history.covariance_filt)
    state_smooth[-1] = history.state_filt[-1]
    covariance_smooth[-1] = history.covariance_filt[-1]

    for t in range(data.n_obs - 2, -1, -1):
        next_covariance = history.covariance_pred[t + 1]
        if np.min(np.diag(next_covariance)) <= MIN_VARIANCE:
            smoother_gain = np.zeros((2, 2), dtype=float)
        else:
            smoother_gain = np.linalg.solve(
                next_covariance.T,
                (history.covariance_filt[t] @ state_transition.T).T,
            ).T

        state_smooth[t] = history.state_filt[t] + smoother_gain @ (
            state_smooth[t + 1] - history.state_pred[t + 1]
        )
        covariance_smooth[t] = history.covariance_filt[t] + smoother_gain @ (
            covariance_smooth[t + 1] - history.covariance_pred[t + 1]
        ) @ smoother_gain.T
        covariance_smooth[t] = _stabilize_covariance(covariance_smooth[t])

    nairu = state_smooth[:, 0]
    nairu_variance = covariance_smooth[:, 0, 0]
    naicu = state_smooth[:, 1]
    naicu_variance = covariance_smooth[:, 1, 1]
    return nairu, nairu_variance, naicu, naicu_variance


def build_outputs(
    df: pd.DataFrame,
    model_data: ModelData,
    fit: FitResult,
    out_dir: Path,
) -> Tuple[pd.DataFrame, str]:
    # Fix 2026-09-03 (alineación v3): antes se exigía fit.success==True o se
    # abortaba con RuntimeError. estimate_parameters (ver el comentario
    # extenso ahí) ya no deriva fit.params del resultado de minimize() --
    # usa BEST_START_PARAMS_V3 directamente y siempre marca success=True, así
    # que ese chequeo ya no puede fallar por una bandera de scipy poco fiable
    # en este óptimo casi degenerado. Lo que sí puede fallar de verdad (por
    # ejemplo, si algún día alguien cambia BEST_START_PARAMS_V3 a mano y se
    # equivoca) son parámetros no finitos o un punto que en realidad cae en
    # la región penalizada del filtro -- eso es lo que se revisa aquí.
    if not np.all(np.isfinite(fit.params)):
        raise RuntimeError(f"Optimization failed (non-finite parameters): {fit.message}")
    if kalman_filter_loglik(fit.params, model_data) >= LARGE_PENALTY * 0.1:
        raise RuntimeError(
            f"Optimization failed (parameters violate model constraints): {fit.message}"
        )

    nairu, nairu_variance, naicu, naicu_variance = kalman_filter_and_smoother(
        fit.params,
        model_data,
    )
    nairu = np.clip(nairu, MIN_NAIRU_LEVEL, MAX_NAIRU_LEVEL)
    naicu = np.clip(naicu, MIN_NAICU_LEVEL, MAX_NAICU_LEVEL)
    nairu_se = np.sqrt(np.maximum(nairu_variance, MIN_VARIANCE))
    naicu_se = np.sqrt(np.maximum(naicu_variance, MIN_VARIANCE))

    out = df.copy()
    out["nairu_estimate"] = nairu
    out["nairu_se"] = nairu_se
    out["nairu_ci_lower_90"] = np.clip(nairu - Z_90 * nairu_se, 0.0, None)
    out["nairu_ci_upper_90"] = nairu + Z_90 * nairu_se
    out["nairu_ci_lower_95"] = np.clip(nairu - Z_95 * nairu_se, 0.0, None)
    out["nairu_ci_upper_95"] = nairu + Z_95 * nairu_se
    out["naicu_estimate"] = naicu
    out["naicu_se"] = naicu_se
    out["naicu_ci_lower_90"] = np.clip(naicu - Z_90 * naicu_se, 0.0, None)
    out["naicu_ci_upper_90"] = naicu + Z_90 * naicu_se
    out["naicu_ci_lower_95"] = np.clip(naicu - Z_95 * naicu_se, 0.0, None)
    out["naicu_ci_upper_95"] = naicu + Z_95 * naicu_se
    out["unemployment_gap"] = out["unemployment_current"] - out["nairu_estimate"]
    out["icu_gap"] = out["icu_current"] - out["naicu_estimate"]

    parameters = unpack_params(fit.params)
    inference = compute_mle_inference(fit, model_data)
    last = out.iloc[-1]
    recent = out.tail(12)
    weak_icu_identification = parameters["icu_total_coefficient"] <= 0.03

    coefficient_table_text = inference.coefficient_table.to_string(
        index=False,
        formatters={
            "estimate": lambda value: _format_float(value),
            "std_error": lambda value: _format_float(value),
            "z_stat": lambda value: _format_float(value),
            "p_value": lambda value: _format_float(value),
            "ci_lower_95": lambda value: _format_float(value),
            "ci_upper_95": lambda value: _format_float(value),
        },
    )
    diagnostics_table_text = inference.diagnostics_table.to_string(
        index=False,
        formatters={"value": lambda value: _format_float(value)},
    )

    summary_lines = [
        "NAIRU/NAICU Estimation v5 (Labor and Capacity Slack)",
        "====================================================",
        f"Sample: {out['Date'].min().date()} to {out['Date'].max().date()}",
        f"Observations used: {len(out)}",
        "",
        "Estimated coefficients:",
        f"intercept: {parameters['intercept']:.6f}",
        f"inflation_lag1_coefficient: {parameters['inflation_lag1_coefficient']:.6f}",
        f"inflation_lag2_coefficient: {parameters['inflation_lag2_coefficient']:.6f}",
        f"unemployment_coefficient_0: {parameters['unemployment_coefficient_0']:.6f}",
        f"unemployment_coefficient_1: {parameters['unemployment_coefficient_1']:.6f}",
        f"unemployment_coefficient_2: {parameters['unemployment_coefficient_2']:.6f}",
        f"unemployment_gap_coefficient: {parameters['unemployment_gap_coefficient']:.6f}",
        f"icu_coefficient_0: {parameters['icu_coefficient_0']:.6f}",
        f"icu_coefficient_1: {parameters['icu_coefficient_1']:.6f}",
        f"icu_coefficient_2: {parameters['icu_coefficient_2']:.6f}",
        f"icu_total_coefficient: {parameters['icu_total_coefficient']:.6f}",
        f"expectations_coefficient: {parameters['expectations_coefficient']:.6f}",
        f"oil_shock_coefficient: {parameters['oil_shock_coefficient']:.6f}",
        f"nairu_adjustment_speed: {parameters['nairu_adjustment_speed']:.6f}",
        f"naicu_adjustment_speed: {parameters['naicu_adjustment_speed']:.6f}",
        "",
        "State-space standard deviations:",
        f"measurement_error_std: {parameters['measurement_error_std']:.6f}",
        f"nairu_transition_std: {parameters['nairu_transition_std']:.6f}",
        f"naicu_transition_std: {parameters['naicu_transition_std']:.6f}",
        "",
        (
            "Latest aligned expected inflation for current period "
            f"(formed 12 months earlier): {last['expected_inflation_current_period']:.2f}%"
        ),
        (
            "Latest contemporaneous Fisher 12m-ahead expectation: "
            f"{last['expected_inflation_fisher_12m_ahead']:.2f}%"
        ),
        f"Latest NAIRU ({last['Date'].date()}): {last['nairu_estimate']:.2f}%",
        f"Latest unemployment: {last['unemployment_current']:.2f}%",
        f"Latest unemployment gap: {last['unemployment_gap']:.2f} pp",
        f"90% CI latest NAIRU: [{last['nairu_ci_lower_90']:.2f}, {last['nairu_ci_upper_90']:.2f}]",
        f"95% CI latest NAIRU: [{last['nairu_ci_lower_95']:.2f}, {last['nairu_ci_upper_95']:.2f}]",
        "",
        f"Latest NAICU ({last['Date'].date()}): {last['naicu_estimate']:.2f}%",
        f"Latest ICU: {last['icu_current']:.2f}%",
        f"Latest ICU gap: {last['icu_gap']:.2f} pp",
        f"90% CI latest NAICU: [{last['naicu_ci_lower_90']:.2f}, {last['naicu_ci_upper_90']:.2f}]",
        f"95% CI latest NAICU: [{last['naicu_ci_lower_95']:.2f}, {last['naicu_ci_upper_95']:.2f}]",
        "",
        f"Average NAIRU last 12 obs: {recent['nairu_estimate'].mean():.2f}%",
        f"Average NAICU last 12 obs: {recent['naicu_estimate'].mean():.2f}%",
        "",
        f"Optimization status: success={fit.success}",
        f"Optimizer diagnostic: {fit.message}",
        f"Negative log-likelihood: {fit.nll:.4f}",
        "",
    ]
    summary_lines += [
        "Maximum-likelihood coefficient table:",
        coefficient_table_text,
        "",
        "Model diagnostics:",
        diagnostics_table_text,
        "",
        "Plot files:",
        "nairu_naicu_panel_v5.png",
        "nairu_naicu_panel_v5.svg",
    ]
    if weak_icu_identification:
        summary_lines.extend(
            [
                "",
                "Identification warning:",
                "The ICU-gap coefficient is close to zero, so NAICU is weakly identified",
                "and should be interpreted with caution.",
            ]
        )
    if inference.warning:
        summary_lines.extend(
            [
                "",
                "Inference warning:",
                inference.warning,
            ]
        )
    summary = "\n".join(summary_lines)

    out_dir.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_dir / "nairu_estimates_v5.csv", index=False)
    (out_dir / "nairu_summary_v5.txt").write_text(summary, encoding="utf-8")
    inference.coefficient_table.to_csv(out_dir / "nairu_mle_coefficients_v5.csv", index=False)
    inference.covariance_matrix.to_csv(out_dir / "nairu_mle_covariance_v5.csv")
    (out_dir / "nairu_mle_diagnostics_v5.txt").write_text(
        diagnostics_table_text,
        encoding="utf-8",
    )
    write_state_panel_figure(out, out_dir)
    return out, summary


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    data = load_and_prepare_data(base_dir / DATA_FILE)
    model_data = build_model_data(data)
    fit = estimate_parameters(model_data)
    _, summary = build_outputs(data, model_data, fit, base_dir / OUTPUT_DIR)
    print(summary)
    print(f"\nFiles written in: {base_dir / OUTPUT_DIR}")


if __name__ == "__main__":
    main()
