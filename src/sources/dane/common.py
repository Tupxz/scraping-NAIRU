"""Utilidades compartidas por los scrapers del DANE.

Centraliza:
  - **Mapas de meses en español** (``MONTH_ABBR_ES``, ``MONTH_FULL_ES``)
    usados al parsear nombres de archivo y celdas Excel del DANE.
  - **Sesión HTTP reutilizable** (``make_dane_session``) con un
    ``urllib3.Retry`` adapter para resistir hipos transitorios del
    servidor del DANE. Reutilizar la sesión evita renegociar el
    handshake TLS en cada request — beneficio notable cuando el mismo
    pipeline hace varias llamadas seguidas (página índice + descarga
    del Excel + posibles retries).

El uso recomendado es::

    session = make_dane_session(headers=config.http_headers)
    html = session.get(config.page_url, timeout=config.timeout).text
    excel = session.get(url, timeout=config.timeout).content

Cualquier duplicación de ``month_map`` en los scrapers de ``src/sources/dane/``
debe migrarse a las constantes de este módulo.
"""

from __future__ import annotations

import os
import warnings
from types import MappingProxyType
from typing import Mapping

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Mapas de meses (canonical, inmutables) ───────────────────────────
# MappingProxyType evita mutaciones accidentales desde los scrapers.

_MONTH_ABBR_ES: dict[str, int] = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

_MONTH_FULL_ES: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

MONTH_ABBR_ES: Mapping[str, int] = MappingProxyType(_MONTH_ABBR_ES)
"""Abreviaturas de mes en español → número (1-12). Inmutable."""

MONTH_FULL_ES: Mapping[str, int] = MappingProxyType(_MONTH_FULL_ES)
"""Nombres completos de mes en español → número (1-12). Inmutable."""


# ── Sesión HTTP reutilizable ─────────────────────────────────────────

def make_dane_session(
    headers: Mapping[str, str] | None = None,
    *,
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Construye una ``requests.Session`` lista para hablar con el DANE.

    - **Retry automático** ante hipos del servidor (5xx, 429) usando
      backoff exponencial.
    - **Reuso de conexión** (keep-alive) para reducir el overhead de
      TLS/TCP en múltiples requests consecutivos al mismo host.
    - **Verificación TLS habilitada por defecto** — no usar
      ``verify=False`` salvo en testing.

    Parameters
    ----------
    headers : Mapping[str, str], optional
        Headers HTTP por defecto que se aplican a todas las requests
        de esta sesión (ej. ``User-Agent``).
    total_retries : int
        Número máximo de reintentos por request (default: 3).
    backoff_factor : float
        Factor multiplicador del delay entre reintentos
        (default: 0.5 → 0.5s, 1s, 2s, ...).
    status_forcelist : tuple[int, ...]
        Códigos HTTP que disparan un reintento.

    Returns
    -------
    requests.Session
        Sesión configurada y lista para usar.
    """
    session = requests.Session()
    if headers:
        session.headers.update(headers)

    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ── Manejo centralizado de TLS contra DANE ──────────────────────────

def dane_request_kwargs(timeout: float = 30.0) -> dict:
    """Kwargs estándar para ``requests.get`` contra ``www.dane.gov.co``.

    DANE tiene problemas recurrentes de cadena de certificados. Permitimos
    saltarse la verificación TLS controlado por la variable de entorno
    ``DANE_VERIFY_TLS`` (default: ``0`` → no verifica). Producción puede
    exportarla a ``1`` cuando DANE arregle el certificado.

    Parameters
    ----------
    timeout : float
        Segundos antes de lanzar ``requests.Timeout`` (default: 30.0).

    Returns
    -------
    dict
        Diccionario listo para pasar como ``**kwargs`` a ``requests.get``.
    """
    verify_tls = os.environ.get("DANE_VERIFY_TLS", "0") == "1"
    if not verify_tls:
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning,
        )
    return {"timeout": timeout, "verify": verify_tls}


__all__ = [
    "MONTH_ABBR_ES",
    "MONTH_FULL_ES",
    "make_dane_session",
    "dane_request_kwargs",
]
