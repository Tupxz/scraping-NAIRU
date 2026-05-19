"""
NAIRU/NAICU estimation v5: expectations-aligned Phillips curve with labor
slack and industrial-capacity slack.

Key extensions vs v4:
1) Adds ICU as an observed slack indicator and estimates a latent NAICU.
2) Uses a two-state Kalman filter with one state for NAIRU and one state for
   NAICU.
3) Keeps the expectation alignment introduced in v4, so period t uses the
   expectation for period t that agents held 12 months earlier.
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
# Período COVID: tratado como shock exógeno a la curva de Phillips,
# no como cambio estructural en la NAIRU
COVID_DUMMY_START = "2020-03-01"
COVID_DUMMY_END   = "2021-06-01"
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
    "icu_gap_coefficient",
    "expectations_coefficient",
    "oil_shock_coefficient",
    "covid_shock_coefficient",     # shock exógeno curva de Phillips 2020-03..2021-06
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
    (0.0, 2.0),     # icu_gap_coefficient
    (-2.0, 2.0),    # expectations_coefficient
    (-2.0, 2.0),    # oil_shock_coefficient
    (-5.0, 5.0),    # covid_shock_coefficient
    (0.0, 0.25),    # nairu_adjustment_speed
    (0.0, 0.25),    # naicu_adjustment_speed
    (-10.0, 4.0),   # log_measurement_error_std
    (-10.0, -0.4),  # log_nairu_transition_std
    (-10.0, 0.7),   # log_naicu_transition_std
    (0.0, 25.0),    # nairu_initial_level
    (50.0, 90.0),   # naicu_initial_level
]
RAW_STD_INDICES = {12, 13, 14}


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
    icu_current: np.ndarray
    icu_lag1: np.ndarray
    expected_inflation_term: np.ndarray
    oil_shock: np.ndarray
    covid_dummy: np.ndarray   # 1 durante COVID_DUMMY_START..END, 0 resto
    n_obs: int


@dataclass(slots=True)
class KalmanHistory:
    state_pred: np.ndarray
    covariance_pred: np.ndarray
    state_filt: np.ndarray
    covariance_filt: np.ndarray


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
    if index == 6:
        step = min(step, 0.45 * max(value - 0.02, 0.0))

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


def _approximate_observed_hessian(
    params: np.ndarray,
    data: ModelData,
) -> Tuple[Optional[np.ndarray], Optional[str]]:
    center_value = kalman_filter_loglik(params, data)
    if not np.isfinite(center_value) or center_value >= LARGE_PENALTY * 0.1:
        return None, "Unable to evaluate the likelihood at the optimum."

    n_params = len(params)
    steps = np.empty(n_params, dtype=float)
    axis_cache: Dict[int, Tuple[float, float]] = {}

    for index in range(n_params):
        selected = _finite_difference_step(params, index, data)
        if selected is None:
            return None, (
                "Observed-information Hessian could not be computed because at least "
                f"one parameter ({PARAMETER_NAMES[index]}) is too close to a bound."
            )
        step, plus_value, minus_value = selected
        steps[index] = step
        axis_cache[index] = (plus_value, minus_value)

    hessian = np.empty((n_params, n_params), dtype=float)
    for index in range(n_params):
        plus_value, minus_value = axis_cache[index]
        hessian[index, index] = (
            plus_value - 2.0 * center_value + minus_value
        ) / (steps[index] * steps[index])

    for i in range(n_params):
        for j in range(i + 1, n_params):
            step_i = steps[i]
            step_j = steps[j]
            evaluation_values = []
            for direction_i in (1.0, -1.0):
                for direction_j in (1.0, -1.0):
                    shifted_params = params.copy()
                    shifted_params[i] += direction_i * step_i
                    shifted_params[j] += direction_j * step_j
                    shifted_value = kalman_filter_loglik(shifted_params, data)
                    evaluation_values.append(shifted_value)

            if (
                not np.all(np.isfinite(evaluation_values))
                or np.max(evaluation_values) >= LARGE_PENALTY * 0.1
            ):
                return None, (
                    "Observed-information Hessian became unstable when evaluating "
                    f"cross derivatives for {PARAMETER_NAMES[i]} and {PARAMETER_NAMES[j]}."
                )

            f_pp, f_pm, f_mp, f_mm = evaluation_values
            cross_derivative = (f_pp - f_pm - f_mp + f_mm) / (4.0 * step_i * step_j)
            hessian[i, j] = cross_derivative
            hessian[j, i] = cross_derivative

    return 0.5 * (hessian + hessian.T), None


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


def compute_mle_inference(fit: FitResult, data: ModelData) -> InferenceResult:
    covariance_matrix: Optional[np.ndarray] = None
    covariance_source = ""
    warning: Optional[str] = None

    observed_hessian, observed_warning = _approximate_observed_hessian(fit.params, data)
    if observed_hessian is not None:
        covariance_matrix, inversion_warning = _invert_information_matrix(observed_hessian)
        if covariance_matrix is not None:
            covariance_source = "Observed-information Hessian (numerical second derivatives)."
            warning = inversion_warning
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
        covariance_matrix = np.full((len(fit.params), len(fit.params)), np.nan, dtype=float)
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

    log_likelihood = -float(fit.nll)
    parameter_count = len(fit.params)
    aic = 2.0 * parameter_count - 2.0 * log_likelihood
    bic = math.log(data.n_obs) * parameter_count - 2.0 * log_likelihood
    diagnostics_table = pd.DataFrame(
        [
            {"metric": "observations", "value": float(data.n_obs), "note": ""},
            {"metric": "estimated_parameters", "value": float(parameter_count), "note": ""},
            {"metric": "log_likelihood", "value": log_likelihood, "note": ""},
            {"metric": "negative_log_likelihood", "value": float(fit.nll), "note": ""},
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

    oil_level = df[OIL_PRICE_COL].where(df[OIL_PRICE_COL] > 0.0)
    df["oil_shock"] = 100.0 * np.log(oil_level).diff()

    df["expected_inflation_term"] = (
        df["expected_inflation_current_period"] - df[CORE_INFLATION_COL].shift(1)
    )

    # Dummy COVID: 1 durante el shock pandémico, 0 resto.
    # Evita que el filtro de Kalman absorba el spike de desempleo
    # 2020-2021 como cambio estructural de la NAIRU.
    df["covid_dummy"] = (
        (df["Date"] >= pd.Timestamp(COVID_DUMMY_START))
        & (df["Date"] <= pd.Timestamp(COVID_DUMMY_END))
    ).astype(float)

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
            "icu_current",
            "icu_lag1",
            "oil_shock",
            "expected_inflation_term",
            "expected_inflation_fisher_12m_ahead",
            "expected_inflation_current_period",
            "covid_dummy",
        ]
    ].dropna().reset_index(drop=True)

    if len(model_df) < 60:
        raise ValueError("Not enough observations after preprocessing.")

    return model_df


def build_model_data(df: pd.DataFrame) -> ModelData:
    return ModelData(
        inflation_gap_change=_to_float_array(df["inflation_gap_change"]),
        inflation_gap_change_lag1=_to_float_array(df["inflation_gap_change_lag1"]),
        inflation_gap_change_lag2=_to_float_array(df["inflation_gap_change_lag2"]),
        unemployment_current=_to_float_array(df["unemployment_current"]),
        unemployment_lag1=_to_float_array(df["unemployment_lag1"]),
        unemployment_lag2=_to_float_array(df["unemployment_lag2"]),
        icu_current=_to_float_array(df["icu_current"]),
        icu_lag1=_to_float_array(df["icu_lag1"]),
        expected_inflation_term=_to_float_array(df["expected_inflation_term"]),
        oil_shock=_to_float_array(df["oil_shock"]),
        covid_dummy=_to_float_array(df["covid_dummy"]),
        n_obs=len(df),
    )


def unpack_params(params: np.ndarray) -> Dict[str, float]:
    (
        intercept,
        inflation_lag1_coefficient,
        inflation_lag2_coefficient,
        unemployment_coefficient_0,
        unemployment_coefficient_1,
        unemployment_coefficient_2,
        icu_gap_coefficient,
        expectations_coefficient,
        oil_shock_coefficient,
        covid_shock_coefficient,
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

    return {
        "intercept": float(intercept),
        "inflation_lag1_coefficient": float(inflation_lag1_coefficient),
        "inflation_lag2_coefficient": float(inflation_lag2_coefficient),
        "unemployment_coefficient_0": float(unemployment_coefficient_0),
        "unemployment_coefficient_1": float(unemployment_coefficient_1),
        "unemployment_coefficient_2": float(unemployment_coefficient_2),
        "unemployment_gap_coefficient": float(unemployment_gap_coefficient),
        "icu_gap_coefficient": float(icu_gap_coefficient),
        "expectations_coefficient": float(expectations_coefficient),
        "oil_shock_coefficient": float(oil_shock_coefficient),
        "covid_shock_coefficient": float(covid_shock_coefficient),
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
    if raw.shape != (17,) or not np.all(np.isfinite(raw)):
        return LARGE_PENALTY, None

    (
        intercept,
        inflation_lag1_coefficient,
        inflation_lag2_coefficient,
        unemployment_coefficient_0,
        unemployment_coefficient_1,
        unemployment_coefficient_2,
        icu_gap_coefficient,
        expectations_coefficient,
        oil_shock_coefficient,
        covid_shock_coefficient,
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

    if unemployment_gap_coefficient >= -0.02:
        penalty = LARGE_PENALTY + 1e6 * (unemployment_gap_coefficient + 0.02) ** 2
        return float(penalty), None
    if icu_gap_coefficient <= 0.02:
        penalty = LARGE_PENALTY + 1e6 * (0.02 - icu_gap_coefficient) ** 2
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
        [-unemployment_gap_coefficient, -icu_gap_coefficient],
        dtype=float,
    )
    measurement_error_variance = measurement_error_std * measurement_error_std
    identity = np.eye(2, dtype=float)

    signal = (
        intercept
        + inflation_lag1_coefficient * data.inflation_gap_change_lag1
        + inflation_lag2_coefficient * data.inflation_gap_change_lag2
        + unemployment_coefficient_0 * data.unemployment_current
        + unemployment_coefficient_1 * data.unemployment_lag1
        + unemployment_coefficient_2 * data.unemployment_lag2
        + icu_gap_coefficient * data.icu_current
        + expectations_coefficient * data.expected_inflation_term
        + oil_shock_coefficient * data.oil_shock
        + covid_shock_coefficient * data.covid_dummy
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

    for t in range(data.n_obs):
        state_control = np.array(
            [
                nairu_adjustment_speed * data.unemployment_lag1[t],
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

        nll += 0.5 * (
            LOG_2PI
            + np.log(innovation_variance)
            + (innovation * innovation) / innovation_variance
        )

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
    )
    return float(nll), history


def kalman_filter_loglik(params: np.ndarray, data: ModelData) -> float:
    nll, _ = _kalman_pass(params, data, store_history=False)
    return nll


def estimate_parameters(data: ModelData) -> FitResult:
    # OLS auxiliar (sin covid_dummy en regresores, solo para arranque)
    x = np.column_stack(
        [
            np.ones(data.n_obs, dtype=float),
            data.inflation_gap_change_lag1,
            data.inflation_gap_change_lag2,
            data.unemployment_current,
            data.unemployment_lag1,
            data.unemployment_lag2,
            data.icu_current,
            data.expected_inflation_term,
            data.oil_shock,
        ]
    )
    ols_coefficients = np.linalg.lstsq(x, data.inflation_gap_change, rcond=None)[0]
    residuals = data.inflation_gap_change - x @ ols_coefficients
    measurement_error_std0 = float(np.std(residuals, ddof=x.shape[1]) + 1e-3)
    # Nivel inicial NAIRU calibrado en 13.5 % para alinearse con el boceto
    # manual (Colombia 2004-2005 tenía desempleo estructural más alto que
    # el promedio 2005-2025 que usaría nanmean).
    nairu_initial_level0 = 13.5
    naicu_initial_level0 = float(np.nanmean(data.icu_current))
    icu_gap_coefficient0 = float(np.clip(abs(ols_coefficients[6]), 0.03, 0.25))

    # x0_base: 17 parámetros (se añadió covid_shock_coefficient en pos 9)
    x0_base = np.array(
        [
            float(ols_coefficients[0]),       # 0  intercept
            float(np.clip(ols_coefficients[1], -0.9, 0.9)),  # 1  lag1
            float(np.clip(ols_coefficients[2], -0.9, 0.9)),  # 2  lag2
            -0.02,                            # 3  unemp_coef_0
            -0.01,                            # 4  unemp_coef_1
            -0.005,                           # 5  unemp_coef_2
            icu_gap_coefficient0,             # 6  icu_gap_coef
            float(ols_coefficients[7]),       # 7  expectations_coef
            float(ols_coefficients[8]),       # 8  oil_shock_coef
            0.0,                              # 9  covid_shock_coef (start en 0)
            0.08,                             # 10 nairu_adj_speed
            0.08,                             # 11 naicu_adj_speed
            float(np.log(max(measurement_error_std0, 1e-3))),  # 12 log_meas_std
            float(np.log(0.05)),              # 13 log_nairu_trans_std
            float(np.log(0.10)),              # 14 log_naicu_trans_std
            nairu_initial_level0,             # 15 nairu_initial_level
            naicu_initial_level0,             # 16 naicu_initial_level
        ]
    )

    starts = [x0_base.copy()]
    # Grid multi-start: varía coeficientes de desempleo, UCI, velocidades y
    # nivel inicial NAIRU (probamos 10, 13.5, 15 para escapar mínimos locales)
    for unemployment_coefficient_0 in [-0.10, -0.06]:
        for unemployment_coefficient_1 in [-0.03]:
            for unemployment_coefficient_2 in [-0.02]:
                for icu_gap_coefficient in [0.04, 0.08]:
                    for nairu_adjustment_speed in [0.05, 0.12]:
                        for naicu_adjustment_speed in [0.05, 0.12]:
                            for naicu_transition_std0 in [0.10, 0.20]:
                                for nairu_init in [10.0, 13.5, 15.0]:
                                    start = x0_base.copy()
                                    start[3]  = unemployment_coefficient_0
                                    start[4]  = unemployment_coefficient_1
                                    start[5]  = unemployment_coefficient_2
                                    start[6]  = icu_gap_coefficient
                                    start[10] = nairu_adjustment_speed
                                    start[11] = naicu_adjustment_speed
                                    start[14] = np.log(naicu_transition_std0)
                                    start[15] = nairu_init
                                    starts.append(start)

    best_success = None
    best_any = None
    for start in starts:
        opt = minimize(
            kalman_filter_loglik,
            x0=start,
            args=(data,),
            method="L-BFGS-B",
            bounds=PARAMETER_BOUNDS,
            options={"maxiter": 1200, "ftol": 1e-8, "gtol": 1e-5},
        )
        if (best_any is None) or (opt.fun < best_any.fun):
            best_any = opt
        if opt.success and ((best_success is None) or (opt.fun < best_success.fun)):
            best_success = opt

    best = best_success if best_success is not None else best_any
    return FitResult(
        params=np.array(best.x, dtype=float),
        success=bool(best.success),
        message=str(best.message),
        nll=float(best.fun),
        optimizer_inverse_hessian=_coerce_inverse_hessian(getattr(best, "hess_inv", None)),
    )


def kalman_filter_and_smoother(
    params: np.ndarray,
    data: ModelData,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    _, history = _kalman_pass(params, data, store_history=True)
    if history is None:
        raise RuntimeError("Kalman filter failed for fitted parameters.")

    state_transition = np.array(
        [
            [1.0 - float(params[9]), 0.0],
            [0.0, 1.0 - float(params[10])],
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
    if not fit.success:
        raise RuntimeError(f"Optimization failed: {fit.message}")

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
    weak_icu_identification = parameters["icu_gap_coefficient"] <= 0.03

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
        f"icu_gap_coefficient: {parameters['icu_gap_coefficient']:.6f}",
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
        f"Optimization status: success={fit.success}, message={fit.message}",
        f"Negative log-likelihood: {fit.nll:.4f}",
        "",
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
