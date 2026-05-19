"""Módulo de cálculo del PIB Potencial Colombia (Cobb-Douglas).

Expone el punto de entrada principal:

    from src.production import run_pib_potencial
    run_pib_potencial.run()

Módulos internos (funciones puras, sin I/O):
    factors         — Factor Trabajo (L) y Capital (K), alpha dinámico
    tfp             — PTF observada y tendencial (filtro HP)
    pib_potencial   — PIB Potencial y brechas (CD + HP)
    excel_writer    — Escritura del Excel multi-hoja de salida
"""
