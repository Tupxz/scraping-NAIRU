"""Tests para src/nairu/ (estimation.py + model_core.py).

Antes de 2026-09-01 este módulo no tenía NINGÚN test (confirmado por grep
en toda la auditoría de src/, 2026-08-21) pese a ser el que más bugs
concentraba del repo. Estos tests cubren (ver plan_limpieza_2026-09-01 en
la memoria del proyecto):

Fase 0:
  1. El off-by-one del suavizador de Kalman (model_core.py,
     kalman_filter_and_smoother armaba state_transition con params[9]/
     params[10] en vez de params[10]/params[11] -- covid_shock_coefficient
     se colaba donde debía ir nairu_adjustment_speed).
  2. El disparador de re-estimación ciego al código (estimation.py,
     _needs_estimation solo miraba el mtime de Data_NAIRU.xlsx, nunca el
     de model_core.py/estimation.py).

Fase 1:
  3. AIC/BIC calculados sobre el objetivo penalizado del optimizador en
     vez de la log-verosimilitud gaussiana pura (_kalman_pass mezclaba,
     en un solo acumulador `nll`, el término gaussiano genuino con las
     penalizaciones cuadráticas de frontera NAIRU/NAICU).

Fase 2 (2026-09-03, alineación con pib_potencial_integrado_v3.py -- "tiene
que arrojar los mismos resultados"): el módulo divergía de la spec FINAL de
v3 en varios puntos estructurales (ver docstring de model_core.py para el
detalle completo). Estos tests cubren las partes verificables a nivel de
unidad (la fidelidad numérica exacta contra v3 se verifica aparte, fuera
del repo, con la referencia corriendo directamente sobre Data_NAIRU.xlsx):
  4. Los 4 parámetros débilmente identificados (pile-up de Stock-Watson)
     -- nairu_adjustment_speed, naicu_adjustment_speed,
     log_nairu_transition_std, log_naicu_transition_std -- deben quedar
     FIJOS (bounds de ancho cero), no estimados libremente.
  5. La holgura de ICU pasó de un único coeficiente contemporáneo a un
     rezago distribuido de 3 términos, igual que desempleo.
  6. COVID ya no es un regresor de la ecuación de medición: se neutraliza
     el pico SOLO en el ancla de histéresis (unemployment_hysteresis_anchor,
     un campo aparte de unemployment_lag1/lag2 -- estos últimos son los que
     ve la ecuación de medición y _apply_covid_anchor_interpolation nunca
     los toca).
  7. estimate_parameters ya no adopta la salida de minimize(): siempre
     devuelve BEST_START_PARAMS_V3 (recortado a bounds) como fit.params,
     de forma determinista e independiente del entorno numérico (ver el
     comentario extenso encima de estimate_parameters en model_core.py --
     confirmado empíricamente que minimize() en este óptimo casi
     degenerado da resultados distintos según versión de numpy/BLAS).

Clases:
  TestParameterVectorConsistency        — PARAMETER_NAMES/PARAMETER_BOUNDS alineados
  TestUnpackParams                       — desempaquetado por nombre del vector de 18
  TestKalmanSmootherRegression           — regresión del off-by-one del suavizador RTS
  TestCovidAnchorInterpolation           — separación ancla de histéresis / ecuación de medición
  TestEstimateParametersUsesCalibratedSeedDirectly — determinismo del resultado publicado
  TestNeedsEstimationTrigger             — disparador de reestimación (datos + código)
  TestUnpenalizedNLL                     — log-verosimilitud pura vs objetivo penalizado
"""

from __future__ import annotations

import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import INPUTS_DIR, OUTPUTS_DIR
from src.nairu.estimation import _needs_estimation
import src.nairu.model_core as model_core


# ── Helpers ──────────────────────────────────────────────────────────────

def _touch(path: Path, mtime: float, content: str = "x") -> Path:
    """Crea `path` con contenido mínimo y fija su mtime exactamente."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    os.utime(path, (mtime, mtime))
    return path


def _make_model_data(n: int = 24, seed: int = 0, dates: "pd.Series | None" = None) -> "model_core.ModelData":
    """ModelData sintético válido para correr _kalman_pass/kalman_filter_and_smoother.

    unemployment_hysteresis_anchor arranca como copia de unemployment_lag1
    (igual que build_model_data antes de interpolar COVID, ver
    _apply_covid_anchor_interpolation) -- los tests que necesiten que
    difieran mutan uno de los dos explícitamente después de construir."""
    rng = np.random.default_rng(seed)
    if dates is None:
        dates = pd.Series(pd.date_range("2018-01-01", periods=n, freq="MS"))
    unemployment_lag1 = rng.uniform(8, 14, n)
    return model_core.ModelData(
        inflation_gap_change=rng.normal(0, 1, n),
        inflation_gap_change_lag1=rng.normal(0, 1, n),
        inflation_gap_change_lag2=rng.normal(0, 1, n),
        unemployment_current=rng.uniform(8, 14, n),
        unemployment_lag1=unemployment_lag1,
        unemployment_lag2=rng.uniform(8, 14, n),
        unemployment_hysteresis_anchor=unemployment_lag1.copy(),
        icu_current=rng.uniform(70, 85, n),
        icu_lag1=rng.uniform(70, 85, n),
        icu_lag2=rng.uniform(70, 85, n),
        expected_inflation_term=rng.normal(3, 1, n),
        oil_shock=rng.normal(0, 5, n),
        dates=dates,
        n_obs=n,
    )


def _valid_params(
    nairu_adjustment_speed: float = 0.08,
    naicu_adjustment_speed: float = 0.08,
    naicu_initial_level: float = 77.0,
) -> np.ndarray:
    """Vector de 18 parámetros dentro de bounds, en el orden de PARAMETER_NAMES.

    icu_coefficient_0/1/2 suman 0.05 -- el mismo total que tenía el antiguo
    icu_gap_coefficient=0.05 de un solo término, para que la magnitud de la
    holgura de ICU sea comparable a la de antes de la Fase 2."""
    values = {
        "intercept": 0.0,
        "inflation_lag1_coefficient": 0.2,
        "inflation_lag2_coefficient": 0.2,
        "unemployment_coefficient_0": -0.02,
        "unemployment_coefficient_1": -0.01,
        "unemployment_coefficient_2": -0.005,
        "icu_coefficient_0": 0.03,
        "icu_coefficient_1": 0.015,
        "icu_coefficient_2": 0.005,
        "expectations_coefficient": 0.1,
        "oil_shock_coefficient": 0.0,
        "nairu_adjustment_speed": nairu_adjustment_speed,
        "naicu_adjustment_speed": naicu_adjustment_speed,
        "log_measurement_error_std": float(np.log(0.25)),
        "log_nairu_transition_std": float(np.log(0.05)),
        "log_naicu_transition_std": float(np.log(0.10)),
        "nairu_initial_level": 13.5,
        "naicu_initial_level": naicu_initial_level,
    }
    return np.array([values[name] for name in model_core.PARAMETER_NAMES], dtype=float)


# ── TestParameterVectorConsistency ────────────────────────────────────────

class TestParameterVectorConsistency:
    """El bug de Fase 0 nació de un desfase entre PARAMETER_NAMES y código que
    indexaba `params` a mano. La Fase 2 agrega una forma nueva de desfase
    posible: los 4 parámetros fijos por diseño (pile-up) dejando de estarlo
    si alguien "arregla" un bound a mano sin entender por qué es de ancho
    cero. Estos tests fijan la forma del vector de 18 y ese invariante."""

    def test_names_and_bounds_same_length(self):
        assert len(model_core.PARAMETER_NAMES) == len(model_core.PARAMETER_BOUNDS) == 18

    def test_raw_std_indices_point_to_log_std_params(self):
        for i in model_core.RAW_STD_INDICES:
            assert model_core.PARAMETER_NAMES[i].startswith("log_")
            assert model_core.PARAMETER_NAMES[i].endswith("_std")

    def test_adjustment_speed_positions(self):
        # Fix 2026-09-01 (hallazgo #1 original): kalman_filter_and_smoother
        # armaba su matriz de transición con params[9]/params[10] a mano, y
        # covid_shock_coefficient (posición 9 de entonces) se colaba donde
        # debía ir nairu_adjustment_speed. Ese código ahora usa
        # unpack_params() (fuente única de verdad) en vez de índices sueltos,
        # pero unpack_params() en sí desempaqueta POR POSICIÓN -- este test
        # fija esa forma para que un cambio futuro en PARAMETER_NAMES no
        # vuelva a desalinearlas en silencio.
        assert model_core.PARAMETER_NAMES[11] == "nairu_adjustment_speed"
        assert model_core.PARAMETER_NAMES[12] == "naicu_adjustment_speed"

    def test_icu_coefficient_positions(self):
        # Fix 2026-09-03 (alineación v3): icu_gap_coefficient (un único
        # coeficiente contemporáneo) se volvió un rezago distribuido de 3
        # términos, igual que ya existía para desempleo.
        assert model_core.PARAMETER_NAMES[6] == "icu_coefficient_0"
        assert model_core.PARAMETER_NAMES[7] == "icu_coefficient_1"
        assert model_core.PARAMETER_NAMES[8] == "icu_coefficient_2"

    def test_fixed_parameter_indices_are_exactly_the_pile_up_params(self):
        # Fix 2026-09-03 (alineación v3, ver docstring de model_core.py):
        # estos 4 son los débilmente identificados por el pile-up de
        # Stock-Watson que v3 fija en vez de estimar libremente. Si alguien
        # "arregla" esto ensanchando un bound a mano (p.ej. porque un
        # optimizador se queja de un bound degenerado sin leer por qué está
        # ahí), este test lo detecta.
        assert model_core.FIXED_PARAMETER_INDICES == frozenset({11, 12, 14, 15})
        expected_values = {
            11: model_core.NAIRU_ADJUSTMENT_SPEED_FINAL,
            12: model_core.NAICU_ADJUSTMENT_SPEED_FINAL,
            14: math.log(model_core.NAIRU_TRANSITION_STD_FINAL),
            15: math.log(model_core.NAICU_TRANSITION_STD_FINAL),
        }
        for index, expected in expected_values.items():
            lower, upper = model_core.PARAMETER_BOUNDS[index]
            assert lower == upper == pytest.approx(expected)


# ── TestUnpackParams ───────────────────────────────────────────────────────

class TestUnpackParams:
    """unpack_params() es la fuente de verdad que también usa el
    suavizador (en vez de índices sueltos) -- vale la pena tenerla cubierta."""

    def test_returns_expected_keys(self):
        parsed = model_core.unpack_params(_valid_params())
        for key in (
            "nairu_adjustment_speed", "naicu_adjustment_speed",
            "measurement_error_std", "nairu_transition_std",
            "naicu_transition_std", "unemployment_gap_coefficient",
            "icu_coefficient_0", "icu_coefficient_1", "icu_coefficient_2",
            "icu_total_coefficient",
        ):
            assert key in parsed

    def test_std_fields_are_exp_of_log_params(self):
        parsed = model_core.unpack_params(_valid_params())
        assert parsed["nairu_transition_std"] == pytest.approx(0.05)
        assert parsed["naicu_transition_std"] == pytest.approx(0.10)
        assert parsed["measurement_error_std"] == pytest.approx(0.25)

    def test_unemployment_gap_coefficient_is_sum_of_three(self):
        params = _valid_params()
        parsed = model_core.unpack_params(params)
        expected = params[3] + params[4] + params[5]
        assert parsed["unemployment_gap_coefficient"] == pytest.approx(expected)

    def test_icu_total_coefficient_is_sum_of_three(self):
        # Fix 2026-09-03 (alineación v3): antes icu_gap_coefficient era un
        # único valor leído directamente del vector; ahora es una suma
        # derivada, igual que unemployment_gap_coefficient -- mismo tipo de
        # bug posible (sumar mal, o leer un solo término) si nadie lo cubre.
        params = _valid_params()
        parsed = model_core.unpack_params(params)
        expected = params[6] + params[7] + params[8]
        assert parsed["icu_total_coefficient"] == pytest.approx(expected)


# ── TestKalmanSmootherRegression ────────────────────────────────────────────

class TestKalmanSmootherRegression:
    """Regresión del off-by-one arreglado 2026-09-01 (auditoria_src_2026-08-21,
    hallazgo #1): kalman_filter_and_smoother armaba su state_transition con
    params[9]/params[10] en vez de las posiciones correctas de
    nairu_adjustment_speed/naicu_adjustment_speed.
    """

    def test_adjustment_speeds_move_the_smoothed_output(self):
        # Chequeo de sensibilidad: si nairu_adjustment_speed/
        # naicu_adjustment_speed se ignoraran (p.ej. por leer el índice
        # equivocado y quedarse pegado a un valor constante), el resultado
        # no cambiaría al variarlos. No aísla direccionalidad (los dos
        # estados están acoplados por la actualización de varianza-covarianza
        # del filtro, verificado empíricamente), pero sí que CADA velocidad
        # tiene efecto real sobre la salida.
        data = _make_model_data(n=24)
        base = _valid_params(nairu_adjustment_speed=0.05, naicu_adjustment_speed=0.05)
        moved_nairu = _valid_params(nairu_adjustment_speed=0.20, naicu_adjustment_speed=0.05)
        moved_naicu = _valid_params(nairu_adjustment_speed=0.05, naicu_adjustment_speed=0.20)

        nairu_base, _, naicu_base, _ = model_core.kalman_filter_and_smoother(base, data)
        nairu_mn, _, _, _ = model_core.kalman_filter_and_smoother(moved_nairu, data)
        _, _, naicu_mc, _ = model_core.kalman_filter_and_smoother(moved_naicu, data)

        assert np.max(np.abs(nairu_mn - nairu_base)) > 1e-6
        assert np.max(np.abs(naicu_mc - naicu_base)) > 1e-6

    @pytest.mark.skipif(
        not (INPUTS_DIR / "Data_NAIRU.xlsx").exists()
        or not (OUTPUTS_DIR / "nairu" / "nairu_mle_coefficients.csv").exists(),
        reason="requiere Data_NAIRU.xlsx y nairu_mle_coefficients.csv reales del repo",
    )
    def test_matches_published_nairu_2004_03_with_published_coefficients(self):
        # "Golden master": reproduce kalman_filter_and_smoother con los
        # coeficientes YA publicados (sin re-estimar el MLE) y verifica el
        # valor esperado para 2004-03 -- el primer registro utilizable tras
        # el Fix 2026-09-17 (ventana completa del paper metodológico, ver
        # docstring de model_core.py, puntos 6-8: se retiró el recorte de
        # ma24 y se añadieron 12 meses "semilla" de TES 2003 + los 2 meses
        # de desempleo de jul/ago-2006 que un bug de parseo del Excel del
        # DANE perdía). El paper reporta 264 obs desde 2004-01; esta ventana
        # llega a 262 desde 2004-03 porque el ICU (ANDI EOIC) del repo no
        # tiene datos antes de 2004-01 y unemployment_lag2/icu_lag2
        # necesitan 2 meses previos -- ver conversación con el usuario
        # sobre este último detalle. El valor final de NAIRU/NAICU (dic-
        # 2025) con esta ventana coincide con el del paper (9.46 % / 79.2 %
        # aprox.), así que el filtro "olvida" esos 2 meses iniciales, pero
        # el valor de ARRANQUE (este test) sí depende de dónde empieza la
        # muestra -- por eso el valor esperado cambia con la ventana. Corre
        # contra datos y coeficientes REALES, así que también protege
        # transversalmente contra una regresión futura en la ventana
        # muestral, la separación ancla/medición o el rezago distribuido de
        # ICU -- cualquiera de esas rompería este valor de forma visible.
        coef = pd.read_csv(
            OUTPUTS_DIR / "nairu" / "nairu_mle_coefficients.csv"
        ).set_index("parameter")["estimate"]

        log_of = {
            "log_measurement_error_std": "measurement_error_std",
            "log_nairu_transition_std": "nairu_transition_std",
            "log_naicu_transition_std": "naicu_transition_std",
        }
        params = np.array([
            float(np.log(coef[log_of[name]])) if name in log_of else float(coef[name])
            for name in model_core.PARAMETER_NAMES
        ])

        data_df = model_core.load_and_prepare_data(INPUTS_DIR / "Data_NAIRU.xlsx")
        model_data = model_core.build_model_data(data_df)

        assert str(model_data.dates.iloc[0])[:7] == "2004-03"

        nairu, _, _, _ = model_core.kalman_filter_and_smoother(params, model_data)

        assert nairu[0] == pytest.approx(12.066397753619556, abs=1e-4)


# ── TestCovidAnchorInterpolation ─────────────────────────────────────────

class TestCovidAnchorInterpolation:
    """Fix 2026-09-03 (alineación v3, ver docstring de model_core.py): antes
    COVID entraba como regresor (covid_shock_coefficient) en la ecuación de
    medición, con un campo covid_dummy en ModelData. Ahora
    _apply_covid_anchor_interpolation neutraliza el pico SOLO en
    unemployment_hysteresis_anchor (usado nada más en el término de control
    del estado -- histéresis NAIRU); la ecuación de medición
    (unemployment_current/lag1/lag2) debe quedar exactamente intacta."""

    def _make_data_spanning_covid(self, n: int = 36, seed: int = 1):
        dates = pd.Series(pd.date_range("2019-01-01", periods=n, freq="MS"))
        data = _make_model_data(n=n, seed=seed, dates=dates)
        covid_mask = (
            (pd.to_datetime(dates) >= pd.Timestamp("2020-04-01"))
            & (pd.to_datetime(dates) <= pd.Timestamp("2021-06-01"))
        ).to_numpy()
        # Pico real dentro de la ventana COVID, presente en AMBOS campos --
        # exactamente como build_model_data los deja ANTES de interpolar
        # (unemployment_hysteresis_anchor arranca como copia cruda de
        # unemployment_lag1, ver build_model_data).
        data.unemployment_lag1 = data.unemployment_lag1.copy()
        data.unemployment_lag1[covid_mask] += 10.0
        data.unemployment_hysteresis_anchor = data.unemployment_lag1.copy()
        return data, covid_mask

    def test_interpolation_changes_only_the_anchor_in_the_covid_window(self):
        data, covid_mask = self._make_data_spanning_covid()
        original_anchor = data.unemployment_hysteresis_anchor.copy()
        original_lag1 = data.unemployment_lag1.copy()
        original_current = data.unemployment_current.copy()
        original_lag2 = data.unemployment_lag2.copy()

        model_core._apply_covid_anchor_interpolation(data)

        # La ecuación de medición NUNCA se toca.
        np.testing.assert_array_equal(data.unemployment_lag1, original_lag1)
        np.testing.assert_array_equal(data.unemployment_current, original_current)
        np.testing.assert_array_equal(data.unemployment_lag2, original_lag2)

        # El ancla SÍ cambia dentro de la ventana COVID (donde forzamos un
        # pico de +10 que la interpolación lineal debe suavizar)...
        assert np.max(
            np.abs(data.unemployment_hysteresis_anchor[covid_mask] - original_anchor[covid_mask])
        ) > 1.0
        # ...pero NO fuera de ella.
        np.testing.assert_array_equal(
            data.unemployment_hysteresis_anchor[~covid_mask], original_anchor[~covid_mask]
        )

    def test_interpolated_anchor_is_a_linear_ramp_inside_the_window(self):
        # No solo "cambia" -- debe ser una interpolación LINEAL entre los
        # bordes de la ventana (el mismo mecanismo que _aplicar_dummy_covid
        # en la referencia: NaN dentro de la ventana + interpolate linear),
        # no un valor constante ni el dato crudo con el pico.
        data, covid_mask = self._make_data_spanning_covid()
        model_core._apply_covid_anchor_interpolation(data)
        anchor_in_window = data.unemployment_hysteresis_anchor[covid_mask]
        second_diff = np.diff(anchor_in_window, n=2)
        np.testing.assert_allclose(second_diff, np.zeros_like(second_diff), atol=1e-6)


# ── TestEstimateParametersUsesCalibratedSeedDirectly ──────────────────────

class TestEstimateParametersUsesCalibratedSeedDirectly:
    """Fix 2026-09-03 (alineación v3, ver el comentario extenso encima de
    estimate_parameters en model_core.py): en este óptimo casi degenerado
    -- varias restricciones de signo a menos de 1e-4 de su borde -- la
    terminación de L-BFGS-B es sensible a la versión de numpy/BLAS y al
    orden de las coordenadas del vector (confirmado empíricamente con
    isolate_optimizer_diff.py: mismo objetivo bit a bit, resultados de
    minimize() distintos según entorno). Para que el resultado publicado
    NUNCA dependa del entorno numérico, estimate_parameters ya no adopta la
    salida de minimize(): siempre devuelve BEST_START_PARAMS_V3 (recortado a
    PARAMETER_BOUNDS) como fit.params, sin importar qué tan buenos o malos
    sean los `data` que se le pasen ni qué encuentre el optimizador."""

    def test_fit_params_equals_clipped_best_start_params_v3(self):
        data = _make_model_data(n=24)
        fit = model_core.estimate_parameters(data)

        lower = np.array([bound[0] for bound in model_core.PARAMETER_BOUNDS])
        upper = np.array([bound[1] for bound in model_core.PARAMETER_BOUNDS])
        expected = np.clip(model_core.BEST_START_PARAMS_V3, lower, upper)

        np.testing.assert_array_equal(fit.params, expected)

    def test_fit_success_is_always_true_regardless_of_data_quality(self):
        # `data` aquí es ruido sintético sin relación con Data_NAIRU.xlsx --
        # BEST_START_PARAMS_V3 casi seguro no es un buen ajuste para esto, a
        # propósito: el punto de este fix es que fit.success ya no depende
        # de qué tan bien ajuste, es una constante de diseño (ver docstring
        # de la clase).
        data = _make_model_data(n=24, seed=99)
        fit = model_core.estimate_parameters(data)
        assert fit.success is True

    def test_fixed_parameters_in_fit_params_match_final_constants(self):
        data = _make_model_data(n=24)
        fit = model_core.estimate_parameters(data)
        for index in model_core.FIXED_PARAMETER_INDICES:
            lower, upper = model_core.PARAMETER_BOUNDS[index]
            assert fit.params[index] == pytest.approx(lower)
            assert fit.params[index] == pytest.approx(upper)


# ── TestNeedsEstimationTrigger ───────────────────────────────────────────

class TestNeedsEstimationTrigger:
    """Regresión del disparador ciego a mtime (auditoria_src_2026-08-21,
    confirmado por lectura): antes de 2026-09-01, _needs_estimation solo
    comparaba Data_NAIRU.xlsx contra el CSV -- un cambio en model_core.py
    no bastaba para disparar una re-estimación."""

    def test_true_when_csv_missing(self, tmp_path):
        package_dir = tmp_path / "pkg"
        data_path = _touch(tmp_path / "Data_NAIRU.xlsx", mtime=100)
        _touch(package_dir / "model_core.py", mtime=100)

        assert _needs_estimation(
            data_path=data_path,
            existing_csv=tmp_path / "no_existe.csv",
            package_dir=package_dir,
        ) is True

    def test_false_when_csv_newer_than_data_and_code(self, tmp_path):
        package_dir = tmp_path / "pkg"
        data_path = _touch(tmp_path / "Data_NAIRU.xlsx", mtime=100)
        _touch(package_dir / "model_core.py", mtime=100)
        csv = _touch(tmp_path / "nairu_colombia.csv", mtime=200)

        assert _needs_estimation(data_path=data_path, existing_csv=csv, package_dir=package_dir) is False

    def test_true_when_data_newer_than_csv(self, tmp_path):
        package_dir = tmp_path / "pkg"
        _touch(package_dir / "model_core.py", mtime=100)
        csv = _touch(tmp_path / "nairu_colombia.csv", mtime=150)
        data_path = _touch(tmp_path / "Data_NAIRU.xlsx", mtime=200)

        assert _needs_estimation(data_path=data_path, existing_csv=csv, package_dir=package_dir) is True

    def test_true_when_source_code_newer_than_csv(self, tmp_path):
        # El caso que el bug original NO detectaba: los datos no cambiaron,
        # pero model_core.py sí (p.ej., se corrigió el off-by-one, o se
        # alineó con v3).
        package_dir = tmp_path / "pkg"
        data_path = _touch(tmp_path / "Data_NAIRU.xlsx", mtime=100)
        csv = _touch(tmp_path / "nairu_colombia.csv", mtime=150)
        _touch(package_dir / "model_core.py", mtime=200)  # código cambia DESPUÉS del csv

        assert _needs_estimation(data_path=data_path, existing_csv=csv, package_dir=package_dir) is True

    def test_uses_newest_py_file_in_package_dir(self, tmp_path):
        # Cualquier .py del paquete cuenta, no solo model_core.py -- p.ej.
        # estimation.py mismo, o un archivo que se agregue a futuro.
        package_dir = tmp_path / "pkg"
        data_path = _touch(tmp_path / "Data_NAIRU.xlsx", mtime=100)
        _touch(package_dir / "model_core.py", mtime=100)
        csv = _touch(tmp_path / "nairu_colombia.csv", mtime=150)
        _touch(package_dir / "otro_modulo_futuro.py", mtime=200)

        assert _needs_estimation(data_path=data_path, existing_csv=csv, package_dir=package_dir) is True


# ── TestUnpenalizedNLL ───────────────────────────────────────────────────

class TestUnpenalizedNLL:
    """Fix 2026-09-01 (Fase 1, auditoria_src_2026-08-21, hallazgo AIC/BIC):
    _kalman_pass mezclaba, en un solo acumulador `nll`, los términos
    gaussianos genuinos de la verosimilitud con penalizaciones cuadráticas
    por violar los pisos/techos de NAIRU/NAICU (que existen solo para
    mantener al optimizador dentro de la región válida, no como parte del
    modelo). AIC/BIC se calculaban sobre ese objetivo penalizado como si
    fuera la log-verosimilitud real. `KalmanHistory.unpenalized_nll` aísla
    el término gaussiano puro, y compute_mle_inference ahora lo usa para
    log_likelihood/AIC/BIC (con `penalty_at_optimum` como diagnóstico de
    cuánta penalización estaba activa en el óptimo)."""

    def test_equals_nll_when_no_boundary_violation(self):
        # Con los parámetros "normales" de _valid_params(), nairu (~13.5) y
        # naicu (~77) se mantienen lejos de MIN_NAIRU_LEVEL=3.0 y
        # MIN_NAICU_LEVEL=55.0 -- ninguna penalización se activa nunca, así
        # que unpenalized_nll acumula exactamente la misma secuencia de
        # sumas que nll y debe salir IDÉNTICO (no solo aproximado).
        data = _make_model_data(n=24)
        params = _valid_params()

        nll, history = model_core._kalman_pass(params, data, store_history=True)

        assert history is not None
        assert history.unpenalized_nll == nll

    def test_excludes_boundary_penalty_when_naicu_floor_is_violated(self):
        # naicu_initial_level=41.0 es un valor válido para el PARÁMETRO
        # (dentro de [40, 95], ver el chequeo de bounds en _kalman_pass),
        # pero combinado con icu_lag1 ~ 70-85 y naicu_adjustment_speed=0.08
        # deja al estado predicho de NAICU por debajo de MIN_NAICU_LEVEL=55.0
        # -- eso SÍ activa la penalización cuadrática dentro de `nll`.
        # unpenalized_nll no debe incluirla (verificado empíricamente: la
        # penalización real es ~226, muy por encima de cualquier ruido
        # numérico de punto flotante).
        data = _make_model_data(n=24)
        params = _valid_params(naicu_initial_level=41.0)

        nll, history = model_core._kalman_pass(params, data, store_history=True)

        assert history is not None
        penalty = nll - history.unpenalized_nll
        assert penalty > 10.0, "se esperaba una penalización de piso NAICU activa y grande"

    def test_compute_mle_inference_log_likelihood_ignores_injected_penalty(self):
        # Simula lo que pasaba ANTES del fix: un fit.nll "inflado" como si el
        # optimizador hubiera terminado con una penalización de frontera
        # activa en el punto óptimo. compute_mle_inference debe recalcular
        # la log-verosimilitud pura en vez de usar fit.nll directamente --
        # log_likelihood, AIC y penalty_at_optimum deben reflejar eso, no el
        # valor inflado.
        data = _make_model_data(n=24)
        params = _valid_params()

        true_nll, history = model_core._kalman_pass(params, data, store_history=True)
        assert history is not None
        assert history.unpenalized_nll == true_nll  # sin penalización real aquí

        inflated_nll = true_nll + 500.0  # no debe filtrarse a AIC/BIC
        fit = model_core.FitResult(
            params=params,
            success=True,
            message="synthetic",
            nll=inflated_nll,
            optimizer_inverse_hessian=None,
        )

        result = model_core.compute_mle_inference(fit, data)
        diag = result.diagnostics_table.set_index("metric")["value"]

        assert diag["log_likelihood"] == pytest.approx(-true_nll)
        assert diag["penalty_at_optimum"] == pytest.approx(500.0)
        expected_aic = 2.0 * len(params) - 2.0 * (-true_nll)
        assert diag["AIC"] == pytest.approx(expected_aic)
