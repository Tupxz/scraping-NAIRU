"""Tests del ICU desde los informes EOIC de la ANDI (offline).

Fixtures: ``tests/fixtures/andi/eoic_<mes><año>_p<n>.pdf`` son páginas reales
de informes EOIC (recortadas con ``pdfseparate`` y sin imágenes, con
ghostscript ``-dFILTERIMAGE -dFILTERVECTOR``: el texto y sus posiciones
quedan intactos).  Cubren las reglas A, C y D, la maquetación a dos
columnas, un párrafo siguiente que también dice "<mes> de <año> se ubicó en
X%" (pedidos), un informe cuya frase es de otro mes y uno sin dato mensual.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from src.sources.andi.eoic import (
    _build_record,
    actualizar_icu,
    es_informe_eoic_pdf,
    resumen_validacion,
    run_andi_pipeline,
    validar_parser_contra_historico,
)
from src.sources.andi.extraccion_icu import (
    ICU_MAX_SALTO,
    ICUControlError,
    ICUExtraccionError,
    candidatos_icu,
    extraer_icu_de_pdf,
    extraer_icu_de_texto,
    mes_en_texto,
    normalizar,
    texto_por_columnas,
    validar_icu_nuevo,
)

FIX = Path(__file__).parent / "fixtures" / "andi"
P = lambda s: pd.Period(s, freq="M")  # noqa: E731


# ═══════════════════════════════════════════════════════════════════════
# Reglas de extracción sobre frases reales de los informes
# ═══════════════════════════════════════════════════════════════════════


class TestReglas:

    @pytest.mark.parametrize("texto, mes, valor, regla", [
        # A (la redacción más común, 2017–2026)
        ("Utilización de la Capacidad Instalada En diciembre de 2025 el indicador de uso de la "
         "capacidad instalada se situó en 77,5%, ubicándose por encima al dato del mismo mes del "
         "año anterior (76,7%).", "2025-12", 77.5, "A"),
        # A con punto decimal (2017)
        ("Utilización de la Capacidad Instalada En abril de 2017 el indicador de uso de la capacidad "
         "instalada se situó en 74.2%, cifra levemente inferior a la observada en el mismo mes del "
         "año anterior (74.9%).", "2017-04", 74.2, "A"),
        # A: "indicador que, en junio de 2025, se situó en"
        ("la actividad empresarial se registra en el uso de la capacidad instalada, indicador que, "
         "en junio de 2025, se situó en 79,9%, levemente superior a lo reportado hace un año (78,9%)",
         "2025-06", 79.9, "A"),
        # A: "alcanzó un nivel de"
        ("La utilización de la capacidad instalada en agosto de 2018 alcanzó un nivel de 81,3%, lo "
         "que sitúa el promedio del año en 79,3%", "2018-08", 81.3, "A"),
        # A: "para el mes de marzo de 2025 este indicador se situó en"
        ("uso de capacidad instalada que no supera el 80%. Este bajo nivel de utilización plantea "
         "retos. Para el mes de marzo de 2025 este indicador se situó en 78,4%, muy similar",
         "2025-03", 78.4, "A"),
        # A: "continuó en niveles moderados al ubicarse en" (mar-2026)
        ("La utilización de la capacidad instalada es un indicador clave para evaluar el nivel de "
         "actividad del sector industrial. Para el mes de marzo de 2026 este indicador continuó en "
         "niveles moderado al ubicarse en 78,2%, prácticamente en", "2026-03", 78.2, "A"),
        # C con fecha explícita
        ("el indicador de uso de la capacidad instalada también se recuperó pasando de 76,7% en "
         "enero de 2018 a 79,6% en enero de 2019, situándose por encima del promedio histórico",
         "2019-01", 79.6, "C"),
        # C "en el mismo mes de este año"
        ("Utilización de la Capacidad Instalada En 2018 el indicador de uso de la capacidad instalada "
         "muestra una sólida recuperación pasando de 76,3% en marzo de 2017 a 79,7% en el mismo mes "
         "de este año, superando así el promedio histórico (76,3%).", "2018-03", 79.7, "C"),
        # D: el mes del dato solo aparece como comparación con el año anterior
        ("Utilización de la Capacidad Instalada El indicador de uso de la capacidad instalada fue de "
         "82,5%, dato superior al registrado en agosto de 2018 (81,4%) y por encima al promedio "
         "histórico (76,3%).", "2019-08", 82.5, "D"),
    ])
    def test_redacciones(self, texto, mes, valor, regla):
        dato = extraer_icu_de_texto(texto, P(mes))
        assert (dato.valor, dato.regla) == (valor, regla)

    def test_cortes_de_palabra_y_mayusculas(self):
        t = "UTILIZACIÓN DE LA CAPACIDAD INSTALADA en julio de 2021 el indicador de uso de la capaci- dad instalada se situó en 81,1%"
        assert extraer_icu_de_texto(t, P("2021-07")).valor == 81.1

    def test_comparaciones_no_son_candidatos(self):
        t = ("el indicador de utilización de la capacidad instalada para el mes de mayo de 2024 se "
             "ubicó en 77,7%, cifra por debajo a lo observado en mayo de 2023 (79,1%) y al promedio")
        assert [(str(c.periodo), c.valor) for c in candidatos_icu(t)] == [("2024-05", 77.7)]

    def test_no_entra_al_parrafo_siguiente(self):
        t = ("utilización de la capacidad instalada en febrero de 2023 el indicador de uso de la "
             "capacidad instalada se situó en 77,3%, menor al observado en el mismo mes del año "
             "anterior (79,3%). Inventarios y pedidos Respecto a los indicadores de inventarios y "
             "pedidos, el volumen de pedidos altos o normales en febrero de 2023 se ubicó en 79,4%")
        assert {c.valor for c in candidatos_icu(t)} == {77.3}

    def test_mes_distinto_es_error_de_control(self):
        t = "utilización de la capacidad instalada en junio de 2020 el indicador de uso de la capacidad instalada se situó en 77,7%"
        with pytest.raises(ICUControlError, match="otro mes"):
            extraer_icu_de_texto(t, P("2020-07"))

    def test_promedio_trimestral_no_es_candidato(self):
        t = ("en el análisis trimestral, la utilización de la capacidad instalada muestra una reducción "
             "frente al mismo periodo del año anterior, al situarse en promedio en 77,6% frente a 78,2% "
             "del primer trimestre de 2025")
        assert candidatos_icu(t) == []

    def test_sin_dato(self):
        t = ("el indicador de utilización de la capacidad instalada se ubica en promedio para 2024 "
             "en 78,3%, reflejando un retroceso respecto a 2023 (79%)")
        with pytest.raises(ICUExtraccionError):
            extraer_icu_de_texto(t, P("2024-10"))

    def test_valores_distintos_para_el_mismo_mes(self):
        t = ("uso de la capacidad instalada en mayo de 2024 se situó en 77,7%. ... "
             "utilización de la capacidad instalada en mayo de 2024 se ubicó en 78,1%")
        with pytest.raises(ICUControlError, match="Valores distintos"):
            extraer_icu_de_texto(t, P("2024-05"))

    @pytest.mark.parametrize("texto, esperado", [
        ("Informe%20EOIC%20Abril%202026.pdf", "2026-04"),
        ("Informe EOIC Enero 2025 MARZO 28 VF.pdf", "2025-01"),
        ("julio / 2021 debemos seguir", "2021-07"),
        ("Septiembre de 2022 la volatilidad", "2022-09"),
        ("Informe EOIC - ANDI - Mayo_638929178751992479.pdf", None),
    ])
    def test_mes_en_texto(self, texto, esperado):
        from urllib.parse import unquote

        r = mes_en_texto(unquote(texto))
        assert (str(r) if r is not None else None) == esperado

    @pytest.mark.parametrize("nombre, es", [
        ("Informe%20EOIC%20Enero%202026.pdf", True),
        ("Informe EOIC - ANDI - Mayo_638929178751992479.pdf", True),
        ("_EOIC F ENERO-2022. BMM gcb.pdf", True),
        ("Infografía EOIC - Abril.pdf", False),
        ("Prensa%20cuadros%20Abril%202019.xlsx.pdf", False),
        ("Informe EOIC Enero 2026.xlsx", False),
    ])
    def test_es_informe_eoic_pdf(self, nombre, es):
        assert es_informe_eoic_pdf(nombre) is es


# ═══════════════════════════════════════════════════════════════════════
# Páginas reales (PDF)
# ═══════════════════════════════════════════════════════════════════════


class TestPDFReales:

    @pytest.mark.parametrize("archivo, mes, valor, regla", [
        ("eoic_dic2025_p3.pdf", "2025-12", 77.5, "A"),
        ("eoic_ene2026_p2.pdf", "2026-01", 76.6, "A"),
        ("eoic_mar2025_p3.pdf", "2025-03", 78.4, "A"),
        ("eoic_mar2026_p3.pdf", "2026-03", 78.2, "A"),   # "… al ubicarse en 78,2%"
        ("eoic_feb2023_p2.pdf", "2023-02", 77.3, "A"),   # sin el 79,4 de pedidos
        ("eoic_mar2018_p4.pdf", "2018-03", 79.7, "C"),
        ("eoic_ago2019_p5.pdf", "2019-08", 82.5, "D"),
    ])
    def test_extrae_el_dato_del_mes(self, archivo, mes, valor, regla):
        dato = extraer_icu_de_pdf(FIX / archivo, P(mes))
        assert (dato.valor, dato.regla) == (valor, regla)

    def test_julio_2020_dice_junio(self):
        with pytest.raises(ICUControlError, match="2020-06=77.7"):
            extraer_icu_de_pdf(FIX / "eoic_jul2020_p3.pdf", P("2020-07"))

    def test_octubre_sin_dato_mensual(self):
        with pytest.raises(ICUExtraccionError):
            extraer_icu_de_pdf(FIX / "eoic_oct2024_p2.pdf", P("2024-10"))

    def test_dos_columnas(self):
        """Por columnas la frase queda contigua; con extract_text de la página completa, no."""
        import pdfplumber

        frase = "en diciembre de 2025 el indicador de uso de la capacidad instalada se situo en 77,5%"
        assert frase in normalizar("\n".join(texto_por_columnas(FIX / "eoic_dic2025_p3.pdf")))
        with pdfplumber.open(FIX / "eoic_dic2025_p3.pdf") as pdf:
            completa = normalizar(pdf.pages[0].extract_text() or "")
        assert frase not in completa


# ═══════════════════════════════════════════════════════════════════════
# Controles de un mes nuevo
# ═══════════════════════════════════════════════════════════════════════


def _serie(hasta: str = "2025-12", ultimo: float = 77.47, n: int = 6) -> pd.Series:
    idx = pd.period_range(end=hasta, periods=n, freq="M")
    return pd.Series([78.0] * (n - 1) + [ultimo], index=idx)


class TestControles:

    def test_pasa(self):
        validar_icu_nuevo(P("2026-01"), 76.6, _serie())

    @pytest.mark.parametrize("valor", [54.9, 90.1])
    def test_rango(self, valor):
        with pytest.raises(ICUControlError, match="fuera del rango"):
            validar_icu_nuevo(P("2026-01"), valor, _serie(ultimo=valor))

    def test_hueco(self):
        with pytest.raises(ICUControlError, match="Falta el ICU de 2025-12"):
            validar_icu_nuevo(P("2026-01"), 76.6, _serie(hasta="2025-11"))

    def test_nunca_sobrescribe(self):
        with pytest.raises(ICUControlError, match="nunca se sobrescribe"):
            validar_icu_nuevo(P("2025-12"), 76.6, _serie())

    def test_salto(self):
        validar_icu_nuevo(P("2026-01"), 70.0 + ICU_MAX_SALTO, _serie(ultimo=70.0))
        with pytest.raises(ICUControlError, match="Salto mensual"):
            validar_icu_nuevo(P("2026-01"), 70.0 + ICU_MAX_SALTO + 0.01, _serie(ultimo=70.0))

    def test_umbral_de_salto_calibrado_con_el_historico(self):
        """Solo COVID (2020) y el paro nacional (2021) superan el umbral en el histórico."""
        from src.config import INPUTS_DIR

        h = pd.read_csv(INPUTS_DIR / "icu_historical.csv", parse_dates=["date"]).set_index("date")
        saltos = h["capacity_utilization"].diff().abs()
        grandes = saltos[saltos > ICU_MAX_SALTO].index.strftime("%Y-%m").tolist()
        assert grandes == ["2020-04", "2020-05", "2021-05", "2021-06"]


# ═══════════════════════════════════════════════════════════════════════
# actualizar_icu y pipeline (con PDF reales y scraper simulado)
# ═══════════════════════════════════════════════════════════════════════


def _no_descargar():
    raise AssertionError("no debía descargarse")


class TestActualizarICU:

    def test_agrega_mes_nuevo_y_no_toca_el_historico(self):
        informes = [
            {"mes": P("2025-12"), "origen": "u/dic", "obtener": _no_descargar},
            {"mes": P("2026-01"), "origen": "u/ene", "obtener": lambda: FIX / "eoic_ene2026_p2.pdf"},
        ]
        nuevos = actualizar_icu(informes, _serie())
        assert [(r["date"], r["capacity_utilization"], r["source"]) for r in nuevos] == [
            ("2026-01-01", 76.6, "u/ene")]

    def test_error_si_falta_un_mes_intermedio(self):
        informes = [{"mes": P("2026-01"), "origen": "u/ene", "obtener": lambda: FIX / "eoic_ene2026_p2.pdf"}]
        with pytest.raises(ICUControlError, match="Falta el ICU de 2025-12"):
            actualizar_icu(informes, _serie(hasta="2025-11"))

    def test_sin_dato_sin_respaldo_es_error(self):
        informes = [{"mes": P("2024-10"), "origen": "u/oct", "obtener": lambda: FIX / "eoic_oct2024_p2.pdf"}]
        with pytest.raises(ICUExtraccionError):
            actualizar_icu(informes, _serie(hasta="2024-09", ultimo=78.9))

    def test_respaldo_pasa_por_los_mismos_controles(self):
        informes = [{"mes": P("2024-10"), "origen": "u/oct", "obtener": lambda: FIX / "eoic_oct2024_p2.pdf"}]
        llamado = {}

        def respaldo(texto, mes):
            llamado["mes"] = mes
            return 78.3, "IA(prueba)"

        nuevos = actualizar_icu(informes, _serie(hasta="2024-09", ultimo=78.9), respaldo_ia=respaldo)
        assert llamado["mes"] == P("2024-10")
        assert nuevos[0]["capacity_utilization"] == 78.3
        assert nuevos[0]["source"] == "IA(prueba)|u/oct"

        with pytest.raises(ICUControlError, match="fuera del rango"):
            actualizar_icu(informes, _serie(hasta="2024-09", ultimo=78.9),
                           respaldo_ia=lambda t, m: (95.0, "IA(prueba)"))

    def test_respaldo_no_se_usa_si_el_parser_encuentra_el_dato(self):
        informes = [{"mes": P("2026-01"), "origen": "u/ene", "obtener": lambda: FIX / "eoic_ene2026_p2.pdf"}]

        def respaldo(texto, mes):
            raise AssertionError("no debía llamarse")

        assert actualizar_icu(informes, _serie(), respaldo_ia=respaldo)[0]["capacity_utilization"] == 76.6

    def test_control_de_mes_no_se_salta_con_respaldo(self):
        informes = [{"mes": P("2020-07"), "origen": "u/jul", "obtener": lambda: FIX / "eoic_jul2020_p3.pdf"}]
        with pytest.raises(ICUControlError):
            actualizar_icu(informes, _serie(hasta="2020-06", ultimo=73.7),
                           respaldo_ia=lambda t, m: (77.7, "IA(prueba)"))


class ScraperFalso:
    def __init__(self, enlaces: dict[str, Path | None]):
        self.enlaces = enlaces
        self.descargas: list[str] = []

    def get_all_eoic_pdfs(self):
        return [{"url": u, "date": mes_en_texto(u.replace("%20", " ")).strftime("%Y-%m")}
                for u in self.enlaces]

    def download_pdf(self, url):
        self.descargas.append(url)
        return self.enlaces[url]


@pytest.fixture
def historico(tmp_path) -> Path:
    p = tmp_path / "icu_historical.csv"
    recs = [_build_record(m, v, "historico") for m, v in
            [("2025-10", 79.8674), ("2025-11", 79.6794), ("2025-12", 77.4729)]]
    pd.DataFrame(recs).to_csv(p, index=False)
    return p


U_DIC = "https://www.andi.com.co/Uploads/Informe%20EOIC%20Diciembre%202025.pdf"
U_ENE = "https://www.andi.com.co/Uploads/Informe%20EOIC%20Enero%202026.pdf"
U_INFO = "https://www.andi.com.co/Uploads/Infograf%C3%ADa%20EOIC%20-%20Enero%202026.pdf"


class TestPipeline:

    def test_historico_mas_mes_nuevo(self, tmp_path, historico):
        sc = ScraperFalso({U_DIC: None, U_ENE: FIX / "eoic_ene2026_p2.pdf", U_INFO: None})
        df = run_andi_pipeline(scraper=sc, csv_path=tmp_path / "andi.csv", historico_path=historico)
        assert df["date"].dt.strftime("%Y-%m").tolist() == ["2025-10", "2025-11", "2025-12", "2026-01"]
        assert df["capacity_utilization"].tolist() == [79.8674, 79.6794, 77.4729, 76.6]
        assert (df["source"].iloc[:3] == "historico").all()
        assert sc.descargas == [U_ENE]          # ni el mes del histórico ni la infografía
        assert not (tmp_path / "andi.csv").exists()   # el pipeline no escribe

    def test_meses_ya_agregados_no_se_descargan(self, tmp_path, historico):
        csv = tmp_path / "andi.csv"
        prev = [_build_record("2026-01", 76.6, U_ENE)]
        pd.concat([pd.read_csv(historico), pd.DataFrame(prev)]).to_csv(csv, index=False)
        sc = ScraperFalso({U_DIC: None, U_ENE: None})
        df = run_andi_pipeline(scraper=sc, csv_path=csv, historico_path=historico)
        assert sc.descargas == []
        assert df["date"].max() == pd.Timestamp("2026-01-01")

        sc2 = ScraperFalso({U_DIC: None, U_ENE: FIX / "eoic_ene2026_p2.pdf"})
        run_andi_pipeline(scraper=sc2, csv_path=csv, historico_path=historico, backfill=True)
        assert sc2.descargas == [U_ENE]

    def test_sin_informes_es_error(self, tmp_path, historico):
        with pytest.raises(RuntimeError, match="No se encontraron informes"):
            run_andi_pipeline(scraper=ScraperFalso({U_INFO: None}),
                              csv_path=tmp_path / "andi.csv", historico_path=historico)

    def test_run_no_escribe_si_falla(self, monkeypatch):
        from src.pipelines import run_andi

        escritos = []
        monkeypatch.setattr(run_andi, "guardar_icu", lambda df: escritos.append(df))

        def falla(**kw):
            raise ICUControlError("salto")

        monkeypatch.setattr(run_andi, "run_andi_pipeline", falla)
        with pytest.raises(ICUControlError):
            run_andi.run()
        assert escritos == []


# ═══════════════════════════════════════════════════════════════════════
# Validación del parser contra el histórico
# ═══════════════════════════════════════════════════════════════════════


def test_validacion_contra_historico(tmp_path):
    d = tmp_path / "andi"
    d.mkdir()
    shutil.copy(FIX / "eoic_dic2025_p3.pdf", d / "Informe EOIC Diciembre 2025.pdf")
    shutil.copy(FIX / "eoic_dic2025_p3.pdf", d / "Informe%20EOIC%20Diciembre%202025.pdf")  # duplicado
    shutil.copy(FIX / "eoic_feb2023_p2.pdf", d / "Informe EOIC Febrero 2023.pdf")
    shutil.copy(FIX / "eoic_jul2020_p3.pdf", d / "Informe EOIC Julio 2020_637375104056325495.pdf")
    shutil.copy(FIX / "eoic_oct2024_p2.pdf", d / "Informe EOIC Octubre 2024.pdf")
    (d / "Informe EOIC Septiembre 2018.pdf").write_text("<html>error</html>")
    hist = tmp_path / "icu.csv"
    pd.DataFrame([_build_record(m, v, "h") for m, v in
                  [("2020-07", 77.7018), ("2023-02", 77.2546), ("2024-10", 78.2991),
                   ("2025-12", 77.4729)]]).to_csv(hist, index=False)

    df = validar_parser_contra_historico(d, hist).set_index("mes")
    assert len(df) == 5                                   # el duplicado cuenta una vez
    assert df.loc["2025-12", "error"] == pytest.approx(0.0271)
    assert df.loc["2023-02", "error"] == pytest.approx(0.0454)
    assert df.loc["2020-07", "estado"] == "control"
    assert df.loc["2024-10", "estado"] == "sin_dato"
    r = resumen_validacion(df.reset_index())
    assert r["comparados"] == 2
    assert r["error_max_abs"] == pytest.approx(0.0454)
    assert r["por_estado"] == {"ok": 2, "control": 1, "sin_dato": 1, "no_pdf": 1}
