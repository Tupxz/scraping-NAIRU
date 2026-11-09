"""Respaldo manual con IA para el ICU (``--andi-ia``, solo local).

Si el extractor de ``extraccion_icu.py`` no encuentra la frase del dato en
un informe EOIC (``ICUExtraccionError``), se le pasa el texto del informe a
``claude -p`` con el modelo más barato (alias ``haiku``) y un prompt mínimo
que pide solo el número.  El número pasa por **los mismos controles** que
el del parser (``validar_icu_nuevo``) y queda marcado en ``source`` como
``IA(claude -p --model haiku)|<url>``.

Nunca corre automáticamente: solo con el flag ``--andi-ia`` de
``src/main.py``, no está en ``.github/workflows/update.yml`` y se niega a
correr si detecta CI (``CI`` o ``GITHUB_ACTIONS`` en el entorno).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import Callable

import pandas as pd

from src.sources.andi.extraccion_icu import ICUExtraccionError

MODELO_IA: str = "haiku"
TIMEOUT_IA: int = 180
PROMPT_IA: str = (
    "Del siguiente texto del informe EOIC de la ANDI de {mes_anio}, devuelve solo el "
    "número de la utilización de capacidad instalada del total de la industria, sin texto "
    "adicional. Si el texto no trae ese dato para ese mes, responde solo NA."
)
_MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
          "septiembre", "octubre", "noviembre", "diciembre"]
_RESPUESTA = re.compile(r"(\d{2}(?:[.,]\d{1,2})?)\s*%?")


def en_ci() -> bool:
    """``True`` si parece una corrida de integración continua (GitHub Actions u otra)."""
    return any(os.environ.get(v, "").lower() in ("1", "true") for v in ("CI", "GITHUB_ACTIONS"))


def comando_claude(prompt: str, modelo: str = MODELO_IA, ejecutable: str = "claude") -> list[str]:
    """Llamada mínima: sin herramientas, sin MCP y sin guardar la sesión."""
    return [ejecutable, "-p", "--model", modelo, "--tools", "", "--strict-mcp-config",
            "--no-session-persistence", prompt]


def consultar_claude(
    texto: str,
    mes: pd.Period,
    *,
    modelo: str = MODELO_IA,
    timeout: int = TIMEOUT_IA,
    ejecutar: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> tuple[float, str]:
    """Pide a ``claude -p`` el ICU del total de la industria de ``mes``.

    El texto del informe va por la entrada estándar; el prompt, como
    argumento.  Devuelve ``(valor, etiqueta)`` para ``source``.

    Raises
    ------
    RuntimeError
        En CI, si no está el ejecutable ``claude`` o si la llamada falla.
    ICUExtraccionError
        Si la respuesta no es solo un número (incluye "NA": el informe no
        trae el dato).
    """
    if en_ci():
        raise RuntimeError("--andi-ia es solo local: no corre en CI / GitHub Actions.")
    ejecutable = shutil.which("claude")
    if ejecutable is None:
        raise RuntimeError("No se encontró el ejecutable 'claude' (Claude Code) en el PATH.")
    prompt = PROMPT_IA.format(mes_anio=f"{_MESES[mes.month - 1]} {mes.year}")
    r = ejecutar(comando_claude(prompt, modelo, ejecutable), input=texto,
                 capture_output=True, text=True, timeout=timeout, check=False)
    if r.returncode != 0:
        raise RuntimeError(f"claude -p falló (código {r.returncode}): {(r.stderr or r.stdout).strip()[:300]}")
    salida = (r.stdout or "").strip()
    m = _RESPUESTA.fullmatch(salida)
    if not m:
        raise ICUExtraccionError(
            f"La IA no devolvió un número para {mes} (respuesta: {salida[:120]!r})."
        )
    return float(m.group(1).replace(",", ".")), f"IA(claude -p --model {modelo})"
