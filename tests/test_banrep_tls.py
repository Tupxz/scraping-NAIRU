"""Tests del arreglo TLS de SUAMECA (cadena incompleta desde 2026-09-11), offline."""

from __future__ import annotations

import hashlib
import ssl
from unittest.mock import MagicMock, patch

import certifi
import requests

from src.sources.banrep.tls import (
    INTERMEDIO_GEOTRUST_EV_RSA_CA_G2,
    AdaptadorTLSBanrep,
    contexto_ssl_banrep,
    montar_tls_banrep,
)

SHA256 = "2D:14:0F:20:B8:A9:6E:2B:4D:2F:1C:C5:AC:A5:E5:A1:E7:DC:56:A7:49:1E:51:09:06:96:0F:38:D2:D2:1A:EF"


def _nombre(rdn) -> dict:
    return {k: v for tupla in rdn for k, v in tupla}


def test_huella_del_intermedio_embebido():
    der = ssl.PEM_cert_to_DER_cert(INTERMEDIO_GEOTRUST_EV_RSA_CA_G2)
    huella = ":".join(f"{b:02X}" for b in hashlib.sha256(der).digest())
    assert huella == SHA256


def test_contexto_incluye_intermedio_y_raices_de_certifi():
    ctx = contexto_ssl_banrep()
    certs = ctx.get_ca_certs()
    sujetos = [_nombre(c["subject"]).get("commonName") for c in certs]
    assert "GeoTrust EV RSA CA G2" in sujetos
    assert "DigiCert Global Root G2" in sujetos          # su emisor, desde certifi
    inter = next(c for c in certs if _nombre(c["subject"]).get("commonName") == "GeoTrust EV RSA CA G2")
    assert _nombre(inter["issuer"])["commonName"] == "DigiCert Global Root G2"
    assert len(certs) > 100                               # no reemplaza a certifi
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname   # verificación completa


def test_raiz_del_intermedio_esta_en_certifi():
    pem = open(certifi.where(), encoding="ascii").read()
    assert "DigiCert Global Root G2" in pem


def test_sesion_monta_el_adaptador():
    s = montar_tls_banrep(requests.Session())
    assert isinstance(s.get_adapter("https://suameca.banrep.gov.co/x"), AdaptadorTLSBanrep)


def test_adaptador_pasa_el_contexto_al_pool():
    a = AdaptadorTLSBanrep()
    assert a.poolmanager.connection_pool_kw["ssl_context"] is a._ctx


def test_create_session_de_ambos_scrapers_usa_el_adaptador():
    from src.sources.banrep import inflation, tes

    for mod in (inflation, tes):
        with patch.object(requests.Session, "get", return_value=MagicMock(content=b"ok")):
            s = mod.create_session()
        assert isinstance(s.get_adapter("https://suameca.banrep.gov.co/"), AdaptadorTLSBanrep)


def test_funciona_en_un_entorno_limpio(tmp_path):
    """Como en GitHub Actions: proceso aislado, sin variables de CA ni HOME del usuario.

    El intermedio va embebido en src/sources/banrep/tls.py (versionado); no
    depende de archivos locales ni de la configuración de esta máquina.
    """
    import os
    import subprocess
    import sys
    from pathlib import Path

    raiz = Path(__file__).resolve().parent.parent
    env = {k: v for k, v in os.environ.items()
           if k not in ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")}
    env["HOME"] = str(tmp_path)
    codigo = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "from src.sources.banrep.tls import contexto_ssl_banrep;"
        "c = contexto_ssl_banrep().get_ca_certs();"
        "n = [dict(x for t in d['subject'] for x in t).get('commonName') for d in c];"
        "print('GeoTrust EV RSA CA G2' in n, 'DigiCert Global Root G2' in n)"
    )
    r = subprocess.run([sys.executable, "-I", "-c", codigo, str(raiz)], env=env, cwd=tmp_path,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "True True"
