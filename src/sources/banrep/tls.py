"""TLS para SUAMECA (Banco de la República): cadena de certificados incompleta.

Diagnóstico (2026-11-09)
------------------------
Desde el 2026-09-11 ``suameca.banrep.gov.co`` usa un certificado nuevo
emitido por *GeoTrust EV RSA CA G2* (DigiCert), pero el servidor envía la
hoja y la raíz *DigiCert Global Root G2* **sin el intermedio**.  Los
navegadores lo descargan solos (extensión AIA); ``requests``/``certifi``
y OpenSSL no, y fallan con ``CERTIFICATE_VERIFY_FAILED: unable to get local
issuer certificate``.  Por eso ``--banrep`` y ``--tes`` fallan desde la
corrida mensual del 2026-10-01 (la primera después del cambio).

Arreglo
-------
Se agrega el intermedio público al almacén de confianza de la sesión de
Banrep, **además** de las raíces de ``certifi``.  La verificación TLS sigue
completa: OpenSSL arma la cadena hoja → intermedio → raíz DigiCert y valida
nombre y fechas (no se usa ``verify=False``).  Si el Banco corrige su
servidor, esto no estorba.

Certificado embebido (descargado de la URL AIA del certificado de SUAMECA,
``http://cacerts.digicert.com/GeoTrustEVRSACAG2.crt``, y verificado con
``openssl verify`` contra ``certifi``):

- Sujeto:  C=US, O=DigiCert Inc, CN=GeoTrust EV RSA CA G2
- Emisor:  C=US, O=DigiCert Inc, OU=www.digicert.com, CN=DigiCert Global Root G2
- Vigencia: 2020-07-02 → **2030-07-02**
- SHA-256: 2D:14:0F:20:B8:A9:6E:2B:4D:2F:1C:C5:AC:A5:E5:A1:E7:DC:56:A7:49:1E:51:09:06:96:0F:38:D2:D2:1A:EF
"""

from __future__ import annotations

import ssl

import certifi
import requests
from requests.adapters import HTTPAdapter

INTERMEDIO_GEOTRUST_EV_RSA_CA_G2 = """\
-----BEGIN CERTIFICATE-----
MIIFPDCCBCSgAwIBAgIQDWASYsinchpuqwNh5WGVgDANBgkqhkiG9w0BAQsFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBH
MjAeFw0yMDA3MDIxMjQyNTdaFw0zMDA3MDIxMjQyNTdaMEQxCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxHjAcBgNVBAMTFUdlb1RydXN0IEVWIFJT
QSBDQSBHMjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANDSvFMMGIOI
D2N+rpyaGTPfyItp3atCyimRJCPtAakU984EB19Zugrb82ui0iy0zLKMDrOuN+Ku
H6jF4F4ZCoHjN7tFdMtSsCUAbrpFVO/5BLY+VTIg7YUN5LwRHUb2QVSGwKyIeYS+
SqPV8mzTMkG9GnWjG1IQT0F1+Xu7+uAGRFVnIdYCSBgl7iG4qOWZjcPKnW+3OJRO
dZtwGE39E7WoasoW8Nvpu5FFp2WIsvY6B/0whr3w+xKeBYb2WNKI1ZXvk4y+AmUa
BdRTY0HPBZ4+6Ipcq2Rl+QLQXQuGXH95YujYNYpbnV7ASU5/dVmqVPY0LHnp2sNK
IN65/n+DbcsCAwEAAaOCAgswggIHMB0GA1UdDgQWBBQo0s/uCYR13bWytb881aDG
c4hdHzAfBgNVHSMEGDAWgBROIlQgGJXm427mD/r6uRLtBhePOTAOBgNVHQ8BAf8E
BAMCAYYwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMBIGA1UdEwEB/wQI
MAYBAf8CAQAwNAYIKwYBBQUHAQEEKDAmMCQGCCsGAQUFBzABhhhodHRwOi8vb2Nz
cC5kaWdpY2VydC5jb20wewYDVR0fBHQwcjA3oDWgM4YxaHR0cDovL2NybDMuZGln
aWNlcnQuY29tL0RpZ2lDZXJ0R2xvYmFsUm9vdEcyLmNybDA3oDWgM4YxaHR0cDov
L2NybDQuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0R2xvYmFsUm9vdEcyLmNybDCBzgYD
VR0gBIHGMIHDMIHABgRVHSAAMIG3MCgGCCsGAQUFBwIBFhxodHRwczovL3d3dy5k
aWdpY2VydC5jb20vQ1BTMIGKBggrBgEFBQcCAjB+DHxBbnkgdXNlIG9mIHRoaXMg
Q2VydGlmaWNhdGUgY29uc3RpdHV0ZXMgYWNjZXB0YW5jZSBvZiB0aGUgUmVseWlu
ZyBQYXJ0eSBBZ3JlZW1lbnQgbG9jYXRlZCBhdCBodHRwczovL3d3dy5kaWdpY2Vy
dC5jb20vcnBhLXVhMA0GCSqGSIb3DQEBCwUAA4IBAQBVbQs8KD6qRnOf/dtOHNAy
ieoRcA1Zk3lGMLwqNfuLpTb9gDQFpvxGN/3X7i0mAEtRhN/r9fDv8oiVLn6gfpm0
UUN47vFJCzf5dqkV/N7LkMTqlhDA0Ve/GBJ3A2E5yFT7TgAGN4wJ9cCZrKbqKHVU
RatwJ+T58jmhbvtEM7t2ucB7u+Dmu4KoOTEx8LR8+qn8OyLh1VLq75vzDTsnhTN5
3AgPdMVFk6F8sjT2P2xbvAb0kAlyAtldAtpGvBzXhacmRs7rTqz/oTDwYZLkrm+q
EltdmGwUOWvFPjwucH6tS+gQgiUYiNJqijzI1jr8TZ01VGOSTW7rRUKgkN5WJqDA
-----END CERTIFICATE-----
"""


def contexto_ssl_banrep() -> ssl.SSLContext:
    """Contexto con las raíces de ``certifi`` más el intermedio de SUAMECA."""
    ctx = ssl.create_default_context(cafile=certifi.where())
    ctx.load_verify_locations(cadata=INTERMEDIO_GEOTRUST_EV_RSA_CA_G2)
    return ctx


class AdaptadorTLSBanrep(HTTPAdapter):
    """``HTTPAdapter`` que usa :func:`contexto_ssl_banrep`."""

    def __init__(self, *args, **kwargs):
        self._ctx = contexto_ssl_banrep()
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ctx
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ctx
        return super().proxy_manager_for(*args, **kwargs)


def montar_tls_banrep(session: requests.Session) -> requests.Session:
    """Monta el adaptador en ``https://`` de la sesión y la devuelve."""
    session.mount("https://", AdaptadorTLSBanrep())
    return session
