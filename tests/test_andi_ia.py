"""Tests del respaldo manual con IA del ICU (``--andi-ia``), sin llamar a claude.

``subprocess.run`` se reemplaza por una función falsa: se verifica el comando
(``claude -p --model haiku`` sin herramientas), que el texto vaya por stdin,
el parseo estricto de la respuesta, que el número pase por los mismos
controles que el parser y que nunca corra en CI.
"""

from __future__ import annotations

import subprocess
from functools import partial
from pathlib import Path

import pandas as pd
import pytest

from src.sources.andi import respaldo_ia
from src.sources.andi.eoic import actualizar_icu
from src.sources.andi.extraccion_icu import ICUControlError, ICUExtraccionError
from src.sources.andi.respaldo_ia import consultar_claude

FIX = Path(__file__).parent / "fixtures" / "andi"
OCT = pd.Period("2024-10", freq="M")


class EjecutarFalso:
    def __init__(self, stdout: str = "78,3\n", returncode: int = 0, stderr: str = ""):
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr
        self.llamadas: list[dict] = []

    def __call__(self, cmd, **kw):
        self.llamadas.append({"cmd": cmd, **kw})
        return subprocess.CompletedProcess(cmd, self.returncode, self.stdout, self.stderr)


@pytest.fixture(autouse=True)
def local(monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setattr(respaldo_ia.shutil, "which", lambda _: "/usr/local/bin/claude")


def test_comando_y_respuesta():
    ej = EjecutarFalso("78,3\n")
    valor, etiqueta = consultar_claude("texto del informe", OCT, ejecutar=ej)
    assert (valor, etiqueta) == (78.3, "IA(claude -p --model haiku)")
    llamada = ej.llamadas[0]
    cmd = llamada["cmd"]
    assert cmd[:4] == ["/usr/local/bin/claude", "-p", "--model", "haiku"]
    assert cmd[cmd.index("--tools") + 1] == ""          # sin herramientas
    assert "--strict-mcp-config" in cmd and "--no-session-persistence" in cmd
    assert "informe EOIC de la ANDI de octubre 2024" in cmd[-1]
    assert "solo el número" in cmd[-1]
    assert llamada["input"] == "texto del informe"      # el texto va por stdin
    assert llamada["timeout"] > 0


@pytest.mark.parametrize("salida, valor", [("77.5", 77.5), ("77,5%", 77.5), (" 80 \n", 80.0)])
def test_respuestas_numericas(salida, valor):
    assert consultar_claude("t", OCT, ejecutar=EjecutarFalso(salida))[0] == valor


@pytest.mark.parametrize("salida", ["NA", "El valor es 77,5%", "77,5 y 78,1", ""])
def test_respuesta_no_numerica_es_error(salida):
    with pytest.raises(ICUExtraccionError, match="no devolvió un número"):
        consultar_claude("t", OCT, ejecutar=EjecutarFalso(salida))


def test_falla_de_la_llamada():
    with pytest.raises(RuntimeError, match="código 1"):
        consultar_claude("t", OCT, ejecutar=EjecutarFalso("", returncode=1, stderr="sin sesión"))


def test_sin_ejecutable(monkeypatch):
    monkeypatch.setattr(respaldo_ia.shutil, "which", lambda _: None)
    with pytest.raises(RuntimeError, match="No se encontró"):
        consultar_claude("t", OCT, ejecutar=EjecutarFalso())


@pytest.mark.parametrize("var", ["CI", "GITHUB_ACTIONS"])
def test_nunca_en_ci(monkeypatch, var):
    monkeypatch.setenv(var, "true")
    ej = EjecutarFalso()
    with pytest.raises(RuntimeError, match="solo local"):
        consultar_claude("t", OCT, ejecutar=ej)
    assert ej.llamadas == []


# ── Integración con el pipeline: mismos controles y marca en source ──────


def _serie_hasta_septiembre() -> pd.Series:
    idx = pd.period_range(end="2024-09", periods=3, freq="M")
    return pd.Series([78.6, 78.7, 78.9], index=idx)


def _informe_octubre():
    return [{"mes": OCT, "origen": "https://andi/Informe%20EOIC%20Octubre%202024.pdf",
             "obtener": lambda: FIX / "eoic_oct2024_p2.pdf"}]


def test_respaldo_en_actualizar_icu():
    ej = EjecutarFalso("78,3")
    nuevos = actualizar_icu(_informe_octubre(), _serie_hasta_septiembre(),
                            respaldo_ia=partial(consultar_claude, ejecutar=ej))
    assert nuevos[0]["capacity_utilization"] == 78.3
    assert nuevos[0]["source"].startswith("IA(claude -p --model haiku)|https://andi/")
    assert "capacidad instalada" in ej.llamadas[0]["input"].lower()   # texto real del informe


def test_respaldo_pasa_por_los_controles():
    with pytest.raises(ICUControlError, match="Salto de"):
        actualizar_icu(_informe_octubre(), _serie_hasta_septiembre(),
                       respaldo_ia=partial(consultar_claude, ejecutar=EjecutarFalso("68,0")))


def test_respaldo_na_deja_el_error_de_extraccion():
    with pytest.raises(ICUExtraccionError):
        actualizar_icu(_informe_octubre(), _serie_hasta_septiembre(),
                       respaldo_ia=partial(consultar_claude, ejecutar=EjecutarFalso("NA")))


# ── CLI y workflow ───────────────────────────────────────────────────────


def test_flag_andi_ia_usa_el_respaldo(monkeypatch):
    from src import main as cli
    from src.pipelines import run_andi

    llamadas = []
    monkeypatch.setattr(run_andi, "run", lambda **kw: llamadas.append(kw))
    monkeypatch.setattr("sys.argv", ["src.main", "--andi-ia"])
    cli.main()
    assert llamadas == [{"backfill": False, "respaldo_ia": consultar_claude}]


def test_flag_andi_ia_falla_en_ci(monkeypatch):
    from src import main as cli
    from src.pipelines import run_andi

    llamadas = []
    monkeypatch.setattr(run_andi, "run", lambda **kw: llamadas.append(kw))
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr("sys.argv", ["src.main", "--andi-ia"])
    with pytest.raises(SystemExit):
        cli.main()
    assert llamadas == []


def test_andi_sin_ia_no_usa_respaldo(monkeypatch):
    from src import main as cli
    from src.pipelines import run_andi

    llamadas = []
    monkeypatch.setattr(run_andi, "run", lambda **kw: llamadas.append(kw))
    monkeypatch.setattr("sys.argv", ["src.main", "--andi"])
    cli.main()
    assert llamadas == [{"backfill": False, "respaldo_ia": None}]


def test_workflow_no_usa_andi_ia():
    wf = (Path(__file__).parent.parent / ".github" / "workflows" / "update.yml").read_text(encoding="utf-8")
    assert "andi-ia" not in wf and "andi_ia" not in wf
