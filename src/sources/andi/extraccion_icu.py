"""Extracción y controles de la utilización de capacidad instalada (ICU).

Sin red: recibe la ruta de un informe mensual EOIC de la ANDI (PDF) o su
texto, y devuelve el dato del **total de la industria** para el mes del
informe.

Cómo se extrae
--------------
1. **Texto por columnas.**  Los informes tienen dos columnas; el texto de
   ``pdfplumber`` sobre la página completa intercala sus líneas.  Para cada
   página se busca un canal vertical sin palabras cerca del centro y se
   extrae cada columna por separado (:func:`texto_por_columnas`).
2. **Frase del dato.**  Tras "uso/utilización de (la) capacidad instalada",
   hasta el inicio de la sección siguiente ("inventarios y pedidos", "clima
   de negocios"…), se buscan las redacciones que usa la ANDI desde 2017
   (:func:`candidatos_icu`):

   - **A** "en <mes> de <año>, … se situó / se ubicó / alcanzó un nivel de X%"
     (incluye "para el mes de <mes> de <año> este indicador se situó en X%"
     y, desde mar-2026, "… continuó en niveles moderados al ubicarse en X%").
   - **C** "pasando de A% en <mes> de <año−1> a X% en <mes> de <año>"
     (o "… en el mismo mes de este año").
   - **D** "el indicador de uso de la capacidad instalada fue de X%, dato …
     registrado en <mes> de <año−1>" → el dato es del mismo mes del año
     siguiente al de la comparación.

   El porcentaje del sector "excluyendo refinación" o los promedios anuales
   no calzan con ninguna regla (no van precedidos de un mes con año).
3. **Mes del dato = mes del informe.**  De los candidatos solo sirven los
   del mes del informe; si los hay de otro mes y ninguno del informe, se
   lanza :class:`ICUControlError`; si no hay ninguno,
   :class:`ICUExtraccionError`.  Si hay varios valores distintos para el
   mes del informe, también es error (ambigüedad).

Controles de un mes nuevo (:func:`validar_icu_nuevo`)
------------------------------------------------------
- Valor en ``[ICU_MIN_NUEVO, ICU_MAX_NUEVO]`` = [55, 90].
- Solo meses posteriores al último de la serie (nunca se modifica un mes
  existente ni se rellena un hueco desde un informe: eso es carga manual en
  ``data/inputs/icu_historical.csv``).  Si queda un hueco, se avisa.
- |valor − último valor disponible| ≤ ``ICU_MAX_SALTO`` = 8 pp.

  Calibración (histórico ANDI 2001-01 → 2025-12, 299 saltos): mediana 1,2;
  p95 3,4; fuera de COVID (mar–jul 2020) y del paro nacional (may–jun 2021)
  el mayor |Δ| es 7,40 pp (ene-2015).  Solo 4 saltos superan 8 pp, todos en
  esos dos episodios (−20,1 abr-2020; +11,4 may-2020; −11,3 may-2021;
  +12,4 jun-2021): un mes así exige revisión manual.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

logger = logging.getLogger("nairu_pipeline.andi")

# ── Controles ──────────────────────────────────────────────────────────
ICU_MIN_NUEVO: float = 55.0
ICU_MAX_NUEVO: float = 90.0
ICU_MAX_SALTO: float = 8.0

MESES: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
_MES = "(?:" + "|".join(MESES) + ")"


class ICUExtraccionError(ValueError):
    """El informe no trae (o no se encontró) la frase con el dato del mes."""


class ICUControlError(ValueError):
    """El dato extraído no pasa un control (mes, rango, salto, huecos)."""


@dataclass(frozen=True)
class DatoICU:
    """Dato de utilización de capacidad instalada extraído de un informe."""

    anio: int
    mes: int
    valor: float
    regla: str
    frase: str

    @property
    def periodo(self) -> pd.Period:
        return pd.Period(year=self.anio, month=self.mes, freq="M")


# ═══════════════════════════════════════════════════════════════════════
# Texto
# ═══════════════════════════════════════════════════════════════════════


def normalizar(texto: str) -> str:
    """Minúsculas, sin tildes, espacios simples, sin cortes de palabra 'ante- rior'."""
    t = unicodedata.normalize("NFKD", texto)
    t = "".join(c for c in t if not unicodedata.combining(c)).lower()
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"([a-z])- ([a-z])", r"\1\2", t)


def _canal_central(pagina) -> float | None:
    """Coordenada x de un canal vertical sin palabras cerca del centro, si existe."""
    palabras = pagina.extract_words()
    if len(palabras) < 50:
        return None
    ancho = pagina.width
    mejor: tuple[float, float] | None = None
    for k in range(60, 141):                      # 30 % … 70 % del ancho
        x = ancho * k / 200
        if any(w["x0"] - 2 <= x <= w["x1"] + 2 for w in palabras):
            continue
        izq = sum(1 for w in palabras if w["x1"] < x)
        der = sum(1 for w in palabras if w["x0"] > x)
        if izq > 20 and der > 20:
            dist = abs(x - ancho / 2)
            if mejor is None or dist < mejor[0]:
                mejor = (dist, x)
    return mejor[1] if mejor else None


def texto_por_columnas(pdf_path: Path) -> list[str]:
    """Texto de cada página, columna por columna (ver docstring del módulo)."""
    import pdfplumber

    paginas: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for p in pdf.pages:
            x = _canal_central(p)
            if x is None:
                paginas.append(p.extract_text() or "")
            else:
                izq = p.crop((0, 0, x, p.height)).extract_text() or ""
                der = p.crop((x, 0, p.width, p.height)).extract_text() or ""
                paginas.append(izq + "\n" + der)
    return paginas


# ═══════════════════════════════════════════════════════════════════════
# Mes del informe
# ═══════════════════════════════════════════════════════════════════════

_FECHA_SUELTA = re.compile(rf"({_MES})\s*(?:/|de\s+|del\s+)?\s*(20\d\d)")


def mes_en_texto(texto: str) -> pd.Period | None:
    """Primer "<mes> [de|/] <año>" del texto (p. ej. nombre de archivo o portada)."""
    m = _FECHA_SUELTA.search(normalizar(texto))
    if not m:
        return None
    return pd.Period(year=int(m.group(2)), month=MESES[m.group(1)], freq="M")


def mes_en_portada(paginas: list[str]) -> pd.Period | None:
    """Mes del encabezado de la primera página ("Diciembre 2025", "julio / 2021")."""
    if not paginas:
        return None
    return mes_en_texto(normalizar(paginas[0])[:300])


# ═══════════════════════════════════════════════════════════════════════
# Frase del dato
# ═══════════════════════════════════════════════════════════════════════

_ANCLA = re.compile(r"(?:uso|utilizacion)\s+de\s+(?:la\s+)?capacidad\s+instalada")
_NUM = r"(?P<valor>\d{2}(?:[.,]\d{1,2})?)\s*%"
_VERBO = (
    r"(?:se\s+situo|se\s+ubico|se\s+ubica|(?:al\s+)?ubicarse|(?:al\s+)?situarse|alcanzo|fue|registro|llego)"
    r"\s+(?:en\s+|de\s+|a\s+)?(?:un\s+nivel\s+de\s+)?"
)
_REGLA_A = re.compile(
    rf"(?P<mes>{_MES})\s+(?:de\s+|del\s+)?(?P<anio>20\d\d)(?P<medio>[^%]{{0,120}}?){_VERBO}{_NUM}"
)
_REGLA_C = re.compile(
    rf"pasando\s+de\s+\d{{2}}(?:[.,]\d{{1,2}})?\s*%\s+en\s+(?P<mes0>{_MES})\s+(?:de\s+)?(?P<anio0>20\d\d)"
    rf"\s+a\s+{_NUM}\s+en\s+(?:(?P<mes>{_MES})\s+(?:de\s+)?(?P<anio>20\d\d)"
    rf"|el\s+mismo\s+mes\s+de\s+(?:este\s+ano|(?P<anio2>20\d\d)))"
)
_REGLA_D = re.compile(
    rf"capacidad\s+instalada\s+fue\s+de\s+{_NUM}[^%]{{0,80}}?(?:registrad[oa]|observad[oa])"
    rf"\s+en\s+(?P<mes>{_MES})\s+(?:de\s+|del\s+)?(?P<anio>20\d\d)"
)
_VENTANA = 420
# Fin del párrafo de capacidad: empieza la sección de otro indicador.
_OTRA_SECCION = re.compile(
    r"inventarios\s+y\s+pedidos|volumen\s+de\s+pedidos|pedidos\s+altos|clima\s+de\s+negocios"
    r"|situacion\s+(?:general|de\s+la\s+empresa)|expectativas"
)


def _ventana(t: str, inicio: int) -> str:
    """Texto desde ``inicio`` hasta ``_VENTANA`` caracteres o el inicio de otra sección."""
    v = t[inicio: inicio + _VENTANA]
    m = _OTRA_SECCION.search(v, 20)
    return v[: m.start()] if m else v


def _a_float(s: str) -> float:
    return float(s.replace(",", "."))


def candidatos_icu(texto: str) -> list[DatoICU]:
    """Todos los (año, mes, valor) que el texto atribuye al uso de capacidad."""
    t = normalizar(texto)
    out: list[DatoICU] = []
    vistos: set[tuple[int, int, float, str]] = set()

    def _agregar(anio: int, mes: int, valor: float, regla: str, frase: str) -> None:
        clave = (anio, mes, valor, regla)
        if clave not in vistos:
            vistos.add(clave)
            out.append(DatoICU(anio, mes, valor, regla, frase.strip()))

    for ancla in _ANCLA.finditer(t):
        ventana = _ventana(t, ancla.start())
        for m in _REGLA_C.finditer(ventana):
            if m.group("mes"):
                anio, mes = int(m.group("anio")), MESES[m.group("mes")]
            else:   # "en el mismo mes de este año / de <año>"
                mes = MESES[m.group("mes0")]
                anio = int(m.group("anio2")) if m.group("anio2") else int(m.group("anio0")) + 1
            _agregar(anio, mes, _a_float(m.group("valor")), "C", m.group(0))
        for m in _REGLA_A.finditer(ventana):
            if "pasando" in m.group("medio") or re.search(r"\d\s*%", m.group("medio")):
                continue
            _agregar(int(m.group("anio")), MESES[m.group("mes")],
                     _a_float(m.group("valor")), "A", m.group(0))
        for m in _REGLA_D.finditer(_ventana(t, max(0, ancla.start() - 40))):
            _agregar(int(m.group("anio")) + 1, MESES[m.group("mes")],
                     _a_float(m.group("valor")), "D", m.group(0))
    return out


def extraer_icu_de_texto(texto: str, mes_informe: pd.Period) -> DatoICU:
    """Dato del mes del informe; error si no está, si es de otro mes o es ambiguo."""
    cands = candidatos_icu(texto)
    del_mes = [c for c in cands if c.periodo == mes_informe]
    if not del_mes:
        if cands:
            otros = sorted({f"{c.periodo}={c.valor}" for c in cands})
            raise ICUControlError(
                f"El informe de {mes_informe} trae la capacidad instalada de otro "
                f"mes ({', '.join(otros)}); el mes del dato debe ser el del informe."
            )
        raise ICUExtraccionError(
            f"No se encontró la utilización de capacidad instalada de {mes_informe} "
            "en el texto del informe."
        )
    valores = {c.valor for c in del_mes}
    if len(valores) > 1:
        raise ICUControlError(
            f"Valores distintos para {mes_informe} en el mismo informe: {sorted(valores)}"
        )
    return del_mes[0]


def extraer_icu_de_pdf(pdf_path: Path, mes_informe: pd.Period | None = None) -> DatoICU:
    """Atajo: texto por columnas + :func:`extraer_icu_de_texto`.

    Si no se da ``mes_informe``, se toma del nombre del archivo y, si no
    trae año, de la portada.
    """
    paginas = texto_por_columnas(pdf_path)
    if mes_informe is None:
        mes_informe = mes_en_texto(pdf_path.name.replace("%20", " ")) or mes_en_portada(paginas)
    if mes_informe is None:
        raise ICUExtraccionError(f"No se pudo determinar el mes del informe {pdf_path.name}.")
    return extraer_icu_de_texto("\n".join(paginas), mes_informe)


# ═══════════════════════════════════════════════════════════════════════
# Controles de un mes nuevo
# ═══════════════════════════════════════════════════════════════════════


def validar_icu_nuevo(
    periodo: pd.Period,
    valor: float,
    serie: pd.Series,
    *,
    minimo: float = ICU_MIN_NUEVO,
    maximo: float = ICU_MAX_NUEVO,
    max_salto: float = ICU_MAX_SALTO,
) -> None:
    """Controles de un mes nuevo frente a la serie existente (índice = Period mensual).

    Solo se agregan meses **al final** (regla del usuario): un mes anterior al
    último de la serie es error.  Si entre el último mes y el nuevo queda un
    hueco (p. ej. octubre y noviembre, que la ANDI no publica), se agrega con
    un aviso y el salto se mide contra el último mes disponible.

    Raises
    ------
    ICUControlError
        Mes ya presente, anterior al último de la serie, fuera de rango o salto
        mayor que ``max_salto`` frente al último mes disponible.
    """
    if periodo in serie.index:
        raise ICUControlError(f"{periodo} ya está en la serie; nunca se sobrescribe.")
    if not (minimo <= valor <= maximo):
        raise ICUControlError(f"ICU {periodo} = {valor} fuera del rango [{minimo}, {maximo}].")
    validos = serie.dropna()
    if validos.empty:
        raise ICUControlError("La serie ICU está vacía: falta el histórico de la ANDI.")
    ultimo = validos.index.max()
    if periodo < ultimo:
        raise ICUControlError(
            f"{periodo} es anterior al último mes de la serie ({ultimo}): solo se agregan meses "
            "al final. Un hueco se completa a mano en data/inputs/icu_historical.csv."
        )
    if periodo - 1 != ultimo:
        faltan = [str(p) for p in pd.period_range(ultimo + 1, periodo - 1, freq="M")]
        logger.warning(
            "[ANDI] Hueco en el ICU: faltan %s antes de %s (cargarlos a mano en "
            "data/inputs/icu_historical.csv). El salto se mide contra %s; Data_NAIRU.xlsx "
            "se trunca en el último mes contiguo.", faltan, periodo, ultimo,
        )
    salto = abs(valor - float(validos.loc[ultimo]))
    if salto > max_salto:
        raise ICUControlError(
            f"Salto de {salto:.2f} pp en {periodo} frente a {ultimo} ({validos.loc[ultimo]} → {valor}); "
            f"máximo permitido {max_salto} pp. Revisar a mano."
        )
