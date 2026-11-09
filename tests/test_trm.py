"""Tests del pipeline de la TRM (SFC vía datos.gov.co).

Todos offline: el portal se simula con ``PortalFalso`` sobre
``tests/fixtures/trm/trm_datos_gov_muestra.csv`` (extracto real de la API,
81 registros, vigencias del 2024-11-28 al 2025-04-01, con fines de semana,
festivos y vigencias que cruzan de mes y de año).

Verifica:
- Paginación ($limit/$offset), detección de descarga truncada.
- Parsing y validaciones del CSV crudo (esquema, unidad, valores, vigencias).
- Continuidad de vigencias y rezago máximo.
- Promedio mensual por días calendario y por registro (cifras a mano).
- Solo meses completos.
- Validaciones de calidad.
- Pipeline completo: no escribe nada si algo falla; copia para legacy/v7.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.config import TRM_CONFIG, TRM_PROCESSED_COLUMNS
from src.pipelines import run_trm
from src.quality_checks import QualityCheckError, run_trm_checks
from src.sources.sfc.trm import (
    descargar_trm,
    expandir_a_dias,
    limpiar_trm,
    parsear_trm_csv,
    promedio_mensual,
    validar_vigencias_continuas,
    verificar_actualidad,
)

FIXTURE = Path(__file__).parent / "fixtures" / "trm" / "trm_datos_gov_muestra.csv"
ENCABEZADO = '"valor","unidad","vigenciadesde","vigenciahasta"'


def _fila(valor: float, desde: str, hasta: str, unidad: str = "COP") -> str:
    return f'"{valor}","{unidad}","{desde}T00:00:00.000","{hasta}T00:00:00.000"'


def _csv(*filas: str) -> str:
    return "\n".join([ENCABEZADO, *filas]) + "\n"


# Caso sintético (cifras a mano):
#   1000  31-ene → 03-feb  (1 día de enero, 3 de febrero)
#   1100  04-feb → 27-feb  (24 días)
#   1200  28-feb → 02-mar  (1 día de febrero, 2 de marzo)
#   1300  03-mar → 31-mar  (29 días)
#   1400  01-abr → 01-abr
# Febrero (28 días) y marzo (31) completos; enero y abril incompletos.
CSV_SINTETICO = _csv(
    _fila(1000, "2025-01-31", "2025-02-03"),
    _fila(1100, "2025-02-04", "2025-02-27"),
    _fila(1200, "2025-02-28", "2025-03-02"),
    _fila(1300, "2025-03-03", "2025-03-31"),
    _fila(1400, "2025-04-01", "2025-04-01"),
)
FEB_CALENDARIO = (3 * 1000 + 24 * 1100 + 1 * 1200) / 28   # 1092.857...
MAR_CALENDARIO = (2 * 1200 + 29 * 1300) / 31              # 1293.548...
FEB_REGISTRO = (1100 + 1200) / 2
MAR_REGISTRO = 1300.0


class PortalFalso:
    """Simula el endpoint Socrata: count(*) y páginas $limit/$offset."""

    def __init__(self, csv_text: str, total: int | None = None,
                 encabezado_pagina: dict[int, str] | None = None):
        lineas = csv_text.strip().splitlines()
        self.encabezado, self.filas = lineas[0], lineas[1:]
        self.total = len(self.filas) if total is None else total
        self.encabezado_pagina = encabezado_pagina or {}
        self.llamadas: list[dict[str, str]] = []

    def __call__(self, url: str, params: dict[str, str]) -> str:
        self.llamadas.append(dict(params))
        if params.get("$select") == "count(*)":
            return f'"count"\n"{self.total}"\n'
        off, lim = int(params["$offset"]), int(params["$limit"])
        enc = self.encabezado_pagina.get(off // lim, self.encabezado)
        return "\n".join([enc, *self.filas[off:off + lim]]) + "\n"


@pytest.fixture
def csv_muestra() -> str:
    return FIXTURE.read_text(encoding="utf-8")


@pytest.fixture
def vigencias_muestra(csv_muestra) -> pd.DataFrame:
    return parsear_trm_csv(csv_muestra)


@pytest.fixture
def vigencias_sinteticas() -> pd.DataFrame:
    return parsear_trm_csv(CSV_SINTETICO)


# ═══════════════════════════════════════════════════════════════════════
# Descarga
# ═══════════════════════════════════════════════════════════════════════


class TestDescarga:

    def test_paginacion(self, csv_muestra):
        portal = PortalFalso(csv_muestra)
        texto = descargar_trm(replace(TRM_CONFIG, page_size=30), portal)
        paginas = [p for p in portal.llamadas if "$offset" in p]
        assert [p["$offset"] for p in paginas] == ["0", "30", "60"]
        assert all(p["$order"].startswith("vigenciadesde") for p in paginas)
        assert texto.strip().splitlines() == csv_muestra.strip().splitlines()

    def test_total_multiplo_del_tamano_de_pagina(self, csv_muestra):
        # 81 = 3 × 27: la cuarta página viene vacía y corta el bucle.
        portal = PortalFalso(csv_muestra)
        texto = descargar_trm(replace(TRM_CONFIG, page_size=27), portal)
        assert len(texto.strip().splitlines()) == 82

    def test_una_sola_pagina(self, csv_muestra):
        portal = PortalFalso(csv_muestra)
        descargar_trm(replace(TRM_CONFIG, page_size=5000), portal)
        assert sum("$offset" in p for p in portal.llamadas) == 1

    def test_truncada_lanza_error(self, csv_muestra):
        portal = PortalFalso(csv_muestra, total=100)
        with pytest.raises(RuntimeError, match="truncada"):
            descargar_trm(replace(TRM_CONFIG, page_size=30), portal)

    def test_encabezado_distinto_lanza_error(self, csv_muestra):
        portal = PortalFalso(csv_muestra, encabezado_pagina={1: '"otra","cosa"'})
        with pytest.raises(RuntimeError, match="Encabezado"):
            descargar_trm(replace(TRM_CONFIG, page_size=30), portal)

    def test_cero_registros_lanza_error(self):
        with pytest.raises(RuntimeError, match="0 registros"):
            descargar_trm(TRM_CONFIG, PortalFalso(_csv()))


# ═══════════════════════════════════════════════════════════════════════
# Parsing y vigencias
# ═══════════════════════════════════════════════════════════════════════


class TestParseo:

    def test_muestra_real(self, vigencias_muestra):
        df = vigencias_muestra
        assert list(df.columns) == ["vigenciadesde", "vigenciahasta", "valor"]
        assert len(df) == 81
        assert df["vigenciadesde"].is_monotonic_increasing
        assert df["valor"].dtype == float
        # Registro que cruza de año: 4409.15 vigente del 31-dic-2024 al 2-ene-2025
        fila = df[df["vigenciadesde"] == "2024-12-31"].iloc[0]
        assert fila["valor"] == 4409.15
        assert fila["vigenciahasta"] == pd.Timestamp("2025-01-02")

    def test_columnas_faltantes(self):
        with pytest.raises(ValueError, match="faltan"):
            parsear_trm_csv('"valor","vigenciadesde"\n"1","2025-01-01"\n')

    def test_sin_filas(self):
        with pytest.raises(ValueError, match="no tiene filas"):
            parsear_trm_csv(_csv())

    def test_unidad_inesperada(self):
        with pytest.raises(ValueError, match="Unidades"):
            parsear_trm_csv(_csv(_fila(4000, "2025-01-01", "2025-01-01", unidad="USD")))

    @pytest.mark.parametrize("valor", ["abc", "0", "-5"])
    def test_valor_invalido(self, valor):
        texto = _csv(f'"{valor}","COP","2025-01-01T00:00:00.000","2025-01-01T00:00:00.000"')
        with pytest.raises(ValueError, match="no numéricos"):
            parsear_trm_csv(texto)

    def test_vigencia_invertida(self):
        with pytest.raises(ValueError, match="hasta < desde"):
            parsear_trm_csv(_csv(_fila(4000, "2025-01-05", "2025-01-03")))

    def test_vigenciadesde_duplicada(self):
        with pytest.raises(ValueError, match="duplicada"):
            parsear_trm_csv(_csv(
                _fila(4000, "2025-01-01", "2025-01-01"),
                _fila(4001, "2025-01-01", "2025-01-02"),
            ))


class TestVigencias:

    def test_muestra_continua(self, vigencias_muestra):
        validar_vigencias_continuas(vigencias_muestra)

    def test_hueco(self):
        df = parsear_trm_csv(_csv(
            _fila(4000, "2025-01-01", "2025-01-02"),
            _fila(4010, "2025-01-04", "2025-01-04"),
        ))
        with pytest.raises(ValueError, match="no continuas"):
            validar_vigencias_continuas(df)

    def test_traslape(self):
        df = parsear_trm_csv(_csv(
            _fila(4000, "2025-01-01", "2025-01-03"),
            _fila(4010, "2025-01-03", "2025-01-04"),
        ))
        with pytest.raises(ValueError, match="no continuas"):
            validar_vigencias_continuas(df)

    def test_actualidad(self, vigencias_muestra):
        # Última vigencia: 2025-04-01
        verificar_actualidad(vigencias_muestra, hoy=date(2025, 4, 16), max_dias_rezago=15)
        with pytest.raises(ValueError, match="desactualizado"):
            verificar_actualidad(vigencias_muestra, hoy=date(2025, 4, 17), max_dias_rezago=15)


# ═══════════════════════════════════════════════════════════════════════
# Agregación mensual
# ═══════════════════════════════════════════════════════════════════════


class TestExpandirADias:

    def test_un_dia_por_fecha(self, vigencias_muestra):
        dias = expandir_a_dias(vigencias_muestra)
        esperado = pd.date_range("2024-11-28", "2025-04-01", freq="D")
        assert dias["fecha"].tolist() == esperado.tolist()

    def test_registro_que_cruza_de_mes(self, vigencias_sinteticas):
        dias = expandir_a_dias(vigencias_sinteticas).set_index("fecha")["trm_cop_usd"]
        assert dias["2025-01-31"] == 1000
        assert dias["2025-02-01":"2025-02-03"].tolist() == [1000] * 3
        assert dias["2025-02-28"] == 1200
        assert dias["2025-03-01":"2025-03-02"].tolist() == [1200] * 2


class TestPromedioMensual:

    def test_dias_calendario_a_mano(self, vigencias_sinteticas):
        out = promedio_mensual(vigencias_sinteticas, "dias_calendario")
        assert out["date"].tolist() == [pd.Timestamp("2025-02-01"), pd.Timestamp("2025-03-01")]
        assert out["trm_cop_usd"].tolist() == pytest.approx([FEB_CALENDARIO, MAR_CALENDARIO])

    def test_registro_a_mano(self, vigencias_sinteticas):
        out = promedio_mensual(vigencias_sinteticas, "registro")
        assert out["trm_cop_usd"].tolist() == pytest.approx([FEB_REGISTRO, MAR_REGISTRO])

    def test_incluir_meses_incompletos(self, vigencias_sinteticas):
        out = promedio_mensual(vigencias_sinteticas, "dias_calendario", solo_meses_completos=False)
        assert out["month"].tolist() == [1, 2, 3, 4]
        assert out["trm_cop_usd"].iloc[0] == 1000
        assert out["trm_cop_usd"].iloc[-1] == 1400

    def test_ponderacion_invalida(self, vigencias_sinteticas):
        with pytest.raises(ValueError, match="ponderacion"):
            promedio_mensual(vigencias_sinteticas, "mediana")

    def test_sin_meses_completos(self):
        df = parsear_trm_csv(_csv(_fila(4000, "2025-01-01", "2025-01-10")))
        with pytest.raises(ValueError, match="ningún mes completo"):
            promedio_mensual(df)

    def test_muestra_real_contra_calculo_independiente(self, vigencias_muestra):
        out = promedio_mensual(vigencias_muestra, "dias_calendario").set_index("date")
        # Recalcular día por día con un bucle simple.
        por_dia: dict[pd.Timestamp, float] = {}
        for _, r in vigencias_muestra.iterrows():
            for d in pd.date_range(r["vigenciadesde"], r["vigenciahasta"], freq="D"):
                por_dia[d] = r["valor"]
        serie = pd.Series(por_dia)
        for mes in ["2024-12", "2025-01", "2025-02", "2025-03"]:
            dias_mes = serie[mes]
            assert len(dias_mes) == pd.Period(mes).days_in_month
            assert out.loc[pd.Timestamp(f"{mes}-01"), "trm_cop_usd"] == pytest.approx(dias_mes.mean())
        # nov-2024 (empieza el 28) y abr-2025 (solo el 1) quedan fuera
        assert out.index.min() == pd.Timestamp("2024-12-01")
        assert out.index.max() == pd.Timestamp("2025-03-01")

    def test_muestra_real_registro_por_vigenciadesde(self, vigencias_muestra):
        out = promedio_mensual(vigencias_muestra, "registro").set_index("date")
        ene = vigencias_muestra[vigencias_muestra["vigenciadesde"].dt.strftime("%Y-%m") == "2025-01"]
        assert out.loc[pd.Timestamp("2025-01-01"), "trm_cop_usd"] == pytest.approx(ene["valor"].mean())
        # 4409.15 (31-dic → 2-ene) cuenta para diciembre, no para enero
        assert 4409.15 not in ene["valor"].tolist()

    def test_esquema(self, vigencias_muestra):
        out = promedio_mensual(vigencias_muestra)
        assert list(out.columns) == ["date", "year", "month", "trm_cop_usd"]
        assert (out["date"].dt.day == 1).all()
        assert out["date"].is_monotonic_increasing


class TestLimpiar:

    def test_columnas_y_metadatos(self, vigencias_muestra):
        cfg = replace(TRM_CONFIG, start_date="2025-01-01")
        out = limpiar_trm(promedio_mensual(vigencias_muestra), cfg, hoy=date(2025, 4, 2))
        assert list(out.columns) == TRM_PROCESSED_COLUMNS
        assert out["date"].min() == pd.Timestamp("2025-01-01")   # recorte por start_date
        assert (out["source"] == "SFC_datos.gov.co").all()
        assert (out["download_date"] == "2025-04-02").all()


# ═══════════════════════════════════════════════════════════════════════
# Validaciones de calidad
# ═══════════════════════════════════════════════════════════════════════


def _df_trm(meses: list[str], valores: list[float] | None = None) -> pd.DataFrame:
    fechas = pd.to_datetime([f"{m}-01" for m in meses])
    return pd.DataFrame({
        "date": fechas, "year": fechas.year, "month": fechas.month,
        "trm_cop_usd": valores or [4000.0] * len(meses),
        "source": "SFC_datos.gov.co", "download_date": "2025-04-02",
    })


class TestTRMQuality:

    def test_pasa(self):
        assert run_trm_checks(_df_trm(["2025-01", "2025-02"]), start_date="2025-01-01")

    def test_fuera_de_rango(self):
        with pytest.raises(QualityCheckError, match="fuera de rango"):
            run_trm_checks(_df_trm(["2025-01", "2025-02"], [4000.0, 40.0]))

    def test_mes_faltante(self):
        with pytest.raises(QualityCheckError, match="Faltan 1 meses"):
            run_trm_checks(_df_trm(["2025-01", "2025-03"]))

    def test_inicio_distinto(self):
        with pytest.raises(QualityCheckError, match="empieza en 2025-02"):
            run_trm_checks(_df_trm(["2025-02", "2025-03"]), start_date="2001-01-01")

    def test_nulo(self):
        with pytest.raises(QualityCheckError, match="nulos"):
            run_trm_checks(_df_trm(["2025-01", "2025-02"], [4000.0, None]))

    def test_columna_faltante(self):
        with pytest.raises(QualityCheckError, match="faltantes"):
            run_trm_checks(_df_trm(["2025-01"]).drop(columns="source"))

    def test_duplicado(self):
        with pytest.raises(QualityCheckError, match="duplicadas"):
            run_trm_checks(_df_trm(["2025-01", "2025-01"]))


# ═══════════════════════════════════════════════════════════════════════
# Pipeline completo (offline)
# ═══════════════════════════════════════════════════════════════════════


class TestPipeline:

    CFG = replace(TRM_CONFIG, start_date="2024-12-01", page_size=30)
    HOY = date(2025, 4, 2)

    @pytest.fixture
    def rutas(self, tmp_path):
        raiz = tmp_path / "repo"
        (raiz / "legacy" / "v7").mkdir(parents=True)
        return {
            "output_dir": tmp_path / "processed",
            "raw_dir": tmp_path / "raw",
            "project_root": raiz,
        }

    def _correr(self, rutas, portal, cfg=None, hoy=None):
        return run_trm.run(config=cfg or self.CFG, obtener=portal, hoy=hoy or self.HOY, **rutas)

    def test_escribe_procesado_crudo_y_copia_v7(self, rutas, csv_muestra):
        df = self._correr(rutas, PortalFalso(csv_muestra))
        procesado = rutas["output_dir"] / "trm_cop_usd_promedio_mensual.csv"
        assert procesado.exists()
        assert (rutas["raw_dir"] / "trm_diaria_raw.csv").exists()

        leido = pd.read_csv(procesado, parse_dates=["date"])
        assert list(leido.columns) == TRM_PROCESSED_COLUMNS
        assert leido["date"].dt.strftime("%Y-%m").tolist() == ["2024-12", "2025-01", "2025-02", "2025-03"]
        assert leido["trm_cop_usd"].tolist() == pytest.approx(df["trm_cop_usd"].tolist())

        # La copia se lee exactamente como lo hace pib_potencial_integrado_v7.py
        copia = rutas["project_root"] / "legacy" / "v7" / "trm_cop_usd_promedio_mensual.csv"
        assert copia.read_text().splitlines()[:2][0] == "date,trm_cop_usd"
        assert copia.read_text().splitlines()[1].startswith("2024-12-01,")
        trm = pd.read_csv(copia, parse_dates=["date"])[["date", "trm_cop_usd"]]
        assert trm["date"].dt.to_period("M").dt.to_timestamp().equals(trm["date"])
        assert trm["trm_cop_usd"].tolist() == pytest.approx(leido["trm_cop_usd"].tolist())

    def test_sin_carpeta_legacy_solo_procesado(self, rutas, csv_muestra, tmp_path):
        rutas["project_root"] = tmp_path / "otra_raiz"
        self._correr(rutas, PortalFalso(csv_muestra))
        assert (rutas["output_dir"] / "trm_cop_usd_promedio_mensual.csv").exists()
        assert not (rutas["project_root"] / "legacy").exists()

    def _sin_salidas(self, rutas):
        assert not (rutas["output_dir"] / "trm_cop_usd_promedio_mensual.csv").exists()
        assert not (rutas["project_root"] / "legacy" / "v7" / "trm_cop_usd_promedio_mensual.csv").exists()

    def test_descarga_truncada_no_escribe(self, rutas, csv_muestra):
        with pytest.raises(RuntimeError, match="truncada"):
            self._correr(rutas, PortalFalso(csv_muestra, total=500))
        self._sin_salidas(rutas)

    def test_fallo_de_calidad_no_escribe(self, rutas, csv_muestra):
        # La muestra empieza en 2024-12: con start_date 2001 la serie está truncada al inicio.
        cfg = replace(self.CFG, start_date="2001-01-01")
        with pytest.raises(QualityCheckError, match="empieza en 2024-12"):
            self._correr(rutas, PortalFalso(csv_muestra), cfg=cfg)
        self._sin_salidas(rutas)

    def test_portal_desactualizado_no_escribe(self, rutas, csv_muestra):
        with pytest.raises(ValueError, match="desactualizado"):
            self._correr(rutas, PortalFalso(csv_muestra), hoy=date(2025, 6, 1))
        self._sin_salidas(rutas)
