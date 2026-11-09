"""Regenera src/v7/motor.py desde legacy/v7/pib_potencial_integrado_v7.py.

Uso: python scripts/sync_motor_v7.py
El motor es una copia literal del script de referencia; el único cambio es que
la raíz de insumos/salidas sale de la variable de entorno V7_ROOT.
"""
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SRC = RAIZ / "legacy" / "v7" / "pib_potencial_integrado_v7.py"
DST = RAIZ / "src" / "v7" / "motor.py"
VIEJO = "_HERE = Path(__file__).resolve().parent\n"
NUEVO = ('_HERE = Path(os.environ["V7_ROOT"]).resolve() if os.environ.get("V7_ROOT") '
         'else Path(__file__).resolve().parent\n')
CABECERA = (
    "# =========================================================================== #\n"
    "#  src/v7/motor.py -- COPIA LITERAL de legacy/v7/pib_potencial_integrado_v7.py  #\n"
    "#                                                                               #\n"
    "#  Unico cambio: la raiz de insumos/salidas (_HERE) se toma de la variable de   #\n"
    "#  entorno V7_ROOT, que fija src/pipelines/run_v7.py despues de armar la        #\n"
    "#  carpeta de insumos con src/v7/insumos.py. No editar la metodologia aqui:     #\n"
    "#  si el script de referencia cambia, regenerar con scripts/sync_motor_v7.py.   #\n"
    "# =========================================================================== #\n"
)


def main() -> None:
    texto = SRC.read_text(encoding="utf-8")
    if texto.count(VIEJO) != 1:
        raise SystemExit("No se encontró exactamente una definición de _HERE en el script v7.")
    DST.write_text(CABECERA + texto.replace(VIEJO, NUEVO), encoding="utf-8")
    print(f"{DST} regenerado desde {SRC}")


if __name__ == "__main__":
    main()
