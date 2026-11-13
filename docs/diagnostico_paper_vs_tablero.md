# Diagnóstico: crecimiento del PIB potencial, paper vs tablero (v7)

2026-11-13. Compara la estimación publicada en el working paper (modo `--referencia`,
`tests/fixtures/baseline_v7/`) con la del tablero (modo repo, `outputs/v7/`, corrida del
2026-11-10). Para reproducirlo: `python scripts/diagnostico_paper_vs_tablero.py` (≈ 4 min). Las
corridas quedan en `data/interim/v7_atribucion/`, que no se versiona. No se modificaron
`src/v7/motor.py`, `outputs/v7/` ni la línea base.

## Resumen

| Crecimiento interanual de `pib_pot` (%, `pct_change(4)`) | 2025T1 | 2025T2 | 2025T3 | 2025T4 | Promedio |
|---|---|---|---|---|---|
| Paper | 3,330 | 2,577 | 2,719 | 1,978 | 2,651 |
| Tablero | 3,263 | 2,501 | 2,642 | 1,885 | 2,573 |
| Diferencia | −0,068 | −0,076 | −0,077 | −0,093 | **−0,078** |

- **No hay diferencia de base, de unidad ni de empalme** (sección 1). Tampoco hay un error nuestro
  con efecto material: con los anexos de la misma edición que usó el paper, nuestros parsers
  reproducen exactamente sus insumos (sección 2).
- **La brecha de −0,08 pp es el neto de dos efectos grandes que van en sentido contrario** (sección 3):
  1. **La revisión del PIB del DANE suma +0,16 pp.** Funciona casi por completo a través del fechado
     del ciclo. Con el PIB revisado, el algoritmo Bry-Boschan encuentra un pico en 2008T3 que con la
     edición del paper descartaba. Lo decide un detalle: el crecimiento de 2009T1 pasó de +0,31 % a
     −0,004 % trimestral.
  2. **Los meses nuevos del bloque mensual (enero a abril de 2026) restan −0,29 pp.** La inflación
     básica sube de 5,0 % a 5,8 % sin que suba el desempleo. El modelo lo lee como un mercado laboral
     más estrecho y el suavizador revisa hacia arriba la NAIRU de 2025: la de diciembre pasa de 8,61 %
     a 8,88 %. Con una NAIRU más alta, el empleo potencial crece menos.
  3. La revisión de la TD desestacionalizada suma +0,05 pp. El resto de los insumos mueve alrededor
     de 0,003 pp en conjunto.
- **Fuera de 2025 el pico de 2008T3 pesa mucho más**: el crecimiento potencial cambia en +1,9 pp por
  año en 2006–2008 y en −0,5 pp en 2010–2014. Sin ese pico, tablero y paper coinciden dentro de
  ±0,05 pp hasta 2023 (sección 3.3). Es lo que se va a ver en la tarjeta nueva del tablero.

La NAIRU que se citó como «8,69 vs 8,88» es el promedio de 2025T4. El dato mensual de diciembre de
2025 es 8,626 % en el paper y 8,880 % en el tablero.

## 1. Base, unidad, muestra y empalme

| Chequeo | Paper | Tablero | ¿Igual? |
|---|---|---|---|
| PIB | Cuadro 4 de `anex-ProduccionConstantes`: desestacionalizado, volumen encadenado, año de referencia 2015, miles de millones | igual (`src/sources/dane/gdp.py`) | sí |
| Índices | 2005T1 = 100 (`BASE_QUARTER` del motor) | igual (mismo motor) | sí |
| Inicio de muestra | 2004T1 trimestral; 2004-01 mensual (264 meses) | 2004T1; 2004-01 (268 meses) | sí (el tablero termina 4 meses después) |
| Empalme 2004 | Tasas de la base 2005 encadenadas: −0,061 / 1,169 / 3,270 / −0,404 % (2004T2 a 2005T1) | mismas tasas (`gdp.empalmar_2004`) | sí |
| Alineación trimestral | Mes de cierre | Mes de inicio, convertido al cierre (`insumos._inicio_trimestre_a_cierre`) | sí: el cociente repo/paper del PIB está en [0,995; 1,005] en los 89 trimestres |
| Último trimestre T | 2026T1 | 2026T2 | no: datos nuevos, efecto ≈ −0,002 pp (sección 3.2) |

## 2. Insumo por insumo

Periodo común de cada par de series. «Desde» es la primera fecha con diferencia; entre paréntesis,
cuántas fechas difieren sobre el total común.

| Insumo | Cobertura paper | Cobertura tablero | Dif. máx. | En | Desde | Origen |
|---|---|---|---|---|---|---|
| `V_pib` | 2004T1–2026T1 | 2004T1–2026T2 | 986 mm (0,50 %) | 2016T1 | 2004T1 (89/89) | **Revisión del DANE** (anexo de 2026T2 frente al de 2026T1; reestimación del ajuste estacional, toda la historia) |
| `X_vapg` | 2005T1–2026T1 | 2005T1–2026T2 | 11 mm (0,19 %) | 2026T1 | 2005T1 (85/85) | Revisión del DANE. Solo fecha T |
| `BQ_ra` | 2016T1–2026T1 | 2014T1–2026T2 | 56 mm (0,04 %) | 2026T1 | 2020T1 (14/41) | Revisión del DANE |
| `BR_tms` | ídem | ídem | 1.744 mm (2,2 %) | 2026T1 | 2016T2 (5/41) | Revisión del DANE. Solo fecha T |
| `BS_ebe` | ídem | ídem | 805 mm (0,59 %) | 2026T1 | 2016T1 (36/41) | Revisión del DANE |
| `BT_im` | ídem | ídem | 113 mm (0,12 %) | 2025T1 | 2016T2 (32/41) | Revisión del DANE. No entra en α (método CBO) |
| `BE_inv` | 2005T1–2026T1 | 2005T1–2026T2 | 2.868 mm (10,6 %) | 2007T2 | 2005T1 | Otra serie de FBKF. **El motor no la usa** |
| `pet` | 2001-01–2026-07 | 2001-01–2026-09 | 50.085 pers. (0,13 %) | 2020-06 | 2001-01 (307/307) | Revisión del DANE (anexo GEIH desestacionalizado de sep-2026 frente al de jul-2026) |
| `fl` | ídem | ídem | 71.478 pers. (0,27 %) | 2025-08 | 2001-01 (307/307) | ídem |
| `ocup` | ídem | ídem | 80.858 pers. (0,34 %) | 2025-08 | 2001-01 (307/307) | ídem |
| TD trimestral (de `fl`, `ocup`) | | | 0,11 pp | 2020T2 | 2004T1 | ídem |
| TGP trimestral | | | 0,03 pp | 2020T2 | 2004T1 | ídem |
| `Unemp_Desest` (TD publicada) | 2001-01–2026-01 | 2001-01–2026-04 | 0,5 pp | 2021-01 | 2001-01 (154/301) | Revisión del DANE (anexo de ene-2026 frente al de sep-2026). Decisión del 2026-11-09: usar la vigente |
| `tgp_desest` | 2001-01–2026-07 | 2001-01–2026-09 | 0,13 pp | 2025-08 | 2001-01 (303/305) | Revisión del DANE (jul-2026 frente a sep-2026) |
| `ICU` (bruto) | 2001-01–2025-12 | 2001-01–2026-04 | 5e-5 | 2022-06 | 2004-01 | Redondeo. Sin efecto |
| ICU trimestral (desestacionalizado con STL) | | | 0,31 pp | 2025T1 | 2004T1 | El STL se reestima con los 4 meses nuevos (datos nuevos, no revisión) |
| `Core_Inf`, `Inf_Rate`, `Inf_Goal` | –2026-02 / –2026-01 | –2026-04 | 0 | — | — | Idénticas. El tablero tiene meses nuevos |
| `Brent_Oil_Price` | 2001-01–2026-03 | 2001-01–2026-04 | 12,1 USD (2026-03, mes fuera de la muestra del paper); hasta 2025-12, máx. 2,5 USD y mediana 0,2 | 2026-03 | 2001-01 | Cambio de fuente: FRED frente a Investing.com (decisión del 2026-11-09). Efecto −0,002 pp |
| `TES_Rate_1yr_COP`/`UVR` | 2003-01–2026-03 | 2003-01–2026-04 | 1,7 / 2,3 pp | 2004-04 / 2007-08 | 2003-01 | Fuente distinta (no es del DANE). **Sin efecto en v7**: el motor solo la usa para recortar la muestra |
| TRM | 2001-01–2026-10 | ídem | 0 | — | — | Idéntica |
| EME (12 meses) | 2003-09–2026-09 | 2003-09–2026-10 | 0,0001 pp | 2018-02 | 2018-02 (3/277) | Redondeo (documentado en [0.6.2]). Sin efecto |
| EMMET, subempleo, vacantes | | | 0 | — | — | Idénticas. El tablero tiene meses nuevos |
| `hc` (PWT 11.0) | 1950–2023 | 1950–2023 | 4,9e-5 | 2002 | 1950 | **Redondeo nuestro**: `pwt.py:364` guarda 4 decimales. Efecto +0,0001 pp (sección 4) |
| Capital (`K_prod_mmp`) | | | 0 | — | — | Mismo archivo |
| Festivos, jornada | | | 0 | — | — | Mismo archivo; la jornada está en el código |

**Cómo se verificó que son revisiones y no errores del scraping.** Se corrieron nuestros parsers
sobre los archivos de la misma edición que usó el paper:

- PIB e ingreso: `parse_gdp_excel` + `empalmar_2004`, `parse_va_petroleo_gas`, `parse_income_excel`
  y `parse_impuestos_netos` sobre `data/raw/dane/dane_gdp_raw.xlsx` y `dane_pib_ingreso_raw.xlsx`.
  Son los anexos de 2026T1 que quedaron en disco con la descarga local del 16-jul-2026; `data/raw`
  no se versiona. Reproducen `V_pib` del paper con diferencia máxima de 5,5e-5 (89 trimestres,
  2004 incluido), `X_vapg` con 4,9e-5, y `BQ_ra`, `BS_ebe`, `BT_im` y `BR_tms` con diferencia 0.
- GEIH: `clean_geih_data` sobre `tests/fixtures/geih/anex-GEIH-Desestacionalizado-jul2026_total_nacional.xlsx`
  reproduce `pet`, `fl` y `ocup` con diferencias de a lo sumo 134 personas (5e-6 relativo; redondeo
  a 0,1 miles). Con el anexo de ene-2026, la TD publicada es idéntica a `Unemp_Desest` del paper.
  Con el de jul-2026, la TGP es idéntica (ya verificado en [0.6.2]).

## 3. Atribución

Cada escenario arma una carpeta de insumos completa y corre el motor sin cambios: paso 1 (NAIRU) y
paso 2 hasta `ejecutar_pipeline`. Los escenarios «paper» y «tablero» reproducen la línea base y
`outputs/v7` con una diferencia de a lo sumo 5e-5 pp en el crecimiento potencial, que es el ruido
del optimizador ya documentado.

### 3.1 Un insumo a la vez

Se parte del paper y se reemplaza **un** insumo por su versión completa del repo (revisiones más
meses nuevos). Las columnas muestran el cambio en el crecimiento interanual de `pib_pot` en pp y la
NAIRU de diciembre de 2025.

| Insumo reemplazado | 2025T1 | 2025T2 | 2025T3 | 2025T4 | Prom. | NAIRU dic-2025 |
|---|---|---|---|---|---|---|
| TD publicada (`Unemp_Desest`) | +0,063 | +0,048 | +0,048 | +0,050 | +0,052 | 8,609 (−0,017) |
| ICU¹ | −0,088 | −0,085 | −0,089 | −0,099 | −0,090 | 8,779 (+0,153) |
| Brent | −0,002 | −0,002 | −0,002 | −0,002 | −0,002 | 8,626 |
| TGP desestacionalizada | −0,001 | −0,001 | 0,000 | 0,000 | 0,000 | 8,625 |
| Inflación, TES, TRM, EME, EMMET, subempleo, vacantes | 0,000 | 0,000 | 0,000 | 0,000 | 0,000 | 8,626 |
| **Los 11 mensuales juntos** | −0,163 | −0,170 | −0,171 | −0,183 | **−0,172** | **8,880 (+0,255)** |
| **PIB** (`V_pib`, `X_vapg`) | +0,148 | +0,147 | +0,148 | +0,147 | **+0,147** | 8,626 |
| Ingreso (`BQ`, `BS`, `BT`, `BR`) | 0,000 | 0,000 | 0,000 | 0,000 | 0,000 | 8,626 |
| GEIH trimestral (`pet`, `fl`, `ocup`) | −0,001 | 0,000 | 0,000 | −0,001 | −0,001 | 8,626 |
| Capital humano | 0,000 | 0,000 | 0,000 | 0,000 | 0,000 | 8,626 |
| *Suma por bloques (mensuales juntos + PIB + ingreso + GEIH + hc)* | | | | | −0,025 | |
| **Todo el repo (tablero)** | −0,068 | −0,076 | −0,077 | −0,093 | **−0,078** | 8,880 (+0,255) |

¹ El ICU bruto es idéntico en el periodo común. Su efecto viene de los 4 meses nuevos: el
ICU era lo que cortaba la muestra del paper en dic-2025, y con el del repo la muestra llega a
ene-2026.

Los efectos uno a uno no son aditivos, por dos razones:

- Dentro del bloque mensual, los 11 reemplazos sueltos suman −0,041 y juntos dan −0,172. La muestra
  solo llega a abr-2026 cuando todas las series llegan a esa fecha.
- Entre bloques, la suma da −0,025 frente a −0,078, porque el pico de 2008T3 cambia cuánto pesa la
  NAIRU más alta.

Por eso la conclusión usa la descomposición exacta de la sección 3.2.

### 3.2 Descomposición exacta: revisiones y datos nuevos

Dos etapas. **A, revisiones**: del paper al repo recortado a la cobertura del paper (mismas fechas,
datos revisados). **B, datos nuevos**: del repo recortado al repo completo. Dentro de cada etapa, las
contribuciones de los 5 bloques son valores de Shapley (32 combinaciones por etapa) y suman
exactamente el cambio de la etapa.

| Bloque | Etapa | 2025T1 | 2025T2 | 2025T3 | 2025T4 | Prom. | NAIRU dic-2025 |
|---|---|---|---|---|---|---|---|
| PIB | A revisiones | +0,156 | +0,155 | +0,156 | +0,155 | **+0,156** | 0 |
| Mensuales | A revisiones | +0,070 | +0,055 | +0,056 | +0,059 | **+0,060** | −0,017 |
| GEIH trimestral | A revisiones | −0,003 | −0,002 | −0,002 | −0,003 | −0,002 | 0 |
| Ingreso, capital humano | A revisiones | 0,000 | 0,000 | 0,000 | 0,000 | 0,000 | 0 |
| *Subtotal A* | | *+0,223* | *+0,209* | *+0,210* | *+0,211* | ***+0,213*** | *−0,017* |
| Mensuales (ene–abr 2026) | B datos nuevos | −0,292 | −0,284 | −0,286 | −0,301 | **−0,290** | +0,271 |
| PIB e ingreso (2026T2: T pasa a 2026T2) | B datos nuevos | 0,000 | −0,002 | −0,002 | −0,003 | −0,002 | 0 |
| *Subtotal B* | | *−0,291* | *−0,285* | *−0,287* | *−0,304* | ***−0,292*** | *+0,271* |
| **Total** | | **−0,068** | **−0,076** | **−0,077** | **−0,093** | **−0,078** | **+0,255** |

Por fuente, en promedio de 2025: **mensuales −0,230** (revisiones +0,060, casi todo de la TD;
meses nuevos −0,290), **PIB +0,155**, GEIH trimestral −0,002, ingreso −0,001, capital humano 0,000.

### 3.3 Mecanismo 1: el pico Bry-Boschan de 2008T3

`detectar_picos_bbq` vuelve a fechar el ciclo en cada corrida (decisión del 2026-11-09, según el
paper):

| | PIB 2008T4 | PIB 2009T1 | Crec. 2009T1 | Valle | Fase contractiva | ¿Pico 2008T3? |
|---|---|---|---|---|---|---|
| Edición del paper (2026T1) | 150 487 | 150 955 | +0,31 % | 2008T4 | 1 trimestre (< mínimo de 2) | descartado |
| Edición vigente (2026T2) | 150 704 | 150 698 | −0,004 % | 2009T1 | 2 trimestres | **se conserva** |

Basta con que 2009T1 quede 6 mil millones de pesos (0,004 %) por debajo de 2008T4 para que aparezca un pico. El pico entra como quiebre de
la tendencia de la PTF y como rampa en la regresión de TGP*. Contrafactual con los picos del paper
(2019T4 y 2023T1) fijos:

| Escenario | Crec. 2025 (prom.) | con picos del paper | Efecto del pico |
|---|---|---|---|
| Paper + PIB del repo | 2,798 | 2,654 | +0,144 |
| Repo recortado (solo revisiones) | 2,864 | 2,705 | +0,159 |
| Tablero | 2,573 | 2,484 | +0,089 |

Sin el pico, la revisión del PIB movería el crecimiento de 2025 apenas +0,003 pp. El mismo pico
mueve mucho más el comienzo de la muestra. Promedios anuales, 100 × Δ4 log:

| Año | Paper | Tablero | Tablero con picos del paper | Dif. | Dif. sin el pico |
|---|---|---|---|---|---|
| 2006 | 4,61 | 6,59 | 4,65 | +1,98 | +0,04 |
| 2008 | 4,80 | 6,61 | 4,80 | +1,82 | 0,00 |
| 2010 | 3,79 | 3,31 | 3,77 | −0,48 | −0,02 |
| 2014 | 5,27 | 4,85 | 5,28 | −0,42 | +0,01 |
| 2019 | 2,14 | 2,28 | 2,15 | +0,14 | +0,01 |
| 2023 | 3,70 | 3,77 | 3,76 | +0,07 | +0,05 |
| 2024 | 1,90 | 1,92 | 1,81 | +0,02 | −0,09 |
| 2025 | 2,62 | 2,54 | 2,45 | −0,08 | −0,16 |

### 3.4 Mecanismo 2: los meses nuevos del bloque mensual

El suavizador usa toda la muestra, así que cada mes nuevo revisa la NAIRU hacia atrás. Se parte del
tablero con el bloque mensual cortado en dic-2025 y se agrega un mes a la vez:

| Muestra mensual hasta | TD | Infl. básica | ICU | Brent | NAIRU dic-2025 | Crec. pot. 2025 | Cambio |
|---|---|---|---|---|---|---|---|
| dic-2025 | 8,4 | 5,02 | 77,5 | 61,8 | 8,609 | 2,863 | — |
| ene-2026 | 8,7 | 5,39 | 76,6 | 64,6 | 8,808 | 2,756 | −0,107 |
| feb-2026 | 8,4 | 5,51 | 78,3 | 69,4 | 8,851 | 2,720 | −0,036 |
| mar-2026 | 8,3 | 5,79 | 78,2 | 99,4 | 8,891 | 2,584 | −0,136 |
| abr-2026 (tablero) | 8,5 | 5,79 | 79,5 | 102,8 | 8,880 | 2,573 | −0,011 |

La NAIRU sube más en 2025 que en 2024 (dic-2024 +0,03; dic-2025 +0,27). Por eso el empleo
potencial crece menos durante 2025: en la descomposición por factores, la contribución del trabajo
al crecimiento potencial de 2025 cae de 1,15 a 0,96 pp. La de la PTF sube de 0,30 a 0,42 pp por el
pico de 2008T3, y la del capital no cambia (1,16 pp).

## 4. ¿Errores nuestros?

**No hay errores con efecto material.** Las diferencias que nacen de nuestro procesamiento son
redondeos:

| Dónde | Qué | Efecto en el crecimiento potencial de 2025 |
|---|---|---|
| `src/sources/pwt/pwt.py:364` | `human_capital` se guarda con 4 decimales; el paper usó la precisión completa de la PWT | +0,0001 pp (`idx_hc` difiere hasta 0,004) |
| `andi_capacidad_instalada.csv` | ICU con ~4 decimales | 0 |
| EME | redondeo binario del quinto decimal (documentado en [0.6.2]) | 0 |

**Propuesta, a la espera de tu OK:** que `pwt.py` no redondee `human_capital`, o lo redondee a 8
decimales, para que el insumo sea idéntico al del paper. No lo recomiendo con urgencia, porque el
efecto es de una diezmilésima de punto. Solo se justifica si se quiere una réplica exacta del paper
en modo repo. No apliqué ningún cambio.

**Para decidir, aunque no es un error:** el fechado Bry-Boschan está en un filo de navaja (sección
3.3). Cumple la regla del paper, pero hace que una revisión ínfima del PIB de 2009T1 mueva hasta 2 pp
el crecimiento potencial de 2006–2008. Hay tres opciones: (a) dejarlo así, como se decidió el
2026-11-09, y documentarlo como sensibilidad; (b) fijar los picos del paper hasta la próxima versión
del documento; (c) proponer al autor del script de referencia una regla de amplitud mínima para las
fases. Cualquiera que no sea (a) es un cambio de metodología.

## 5. Conclusión

El tablero estima un crecimiento potencial de 2025 unos 0,08 pp menor que el del paper. No hay
diferencias de base, de unidades ni de empalme, y los programas que bajan los datos reproducen
exactamente lo que usó el paper cuando se les da la misma edición de las cifras. La diferencia
sale de usar datos más nuevos, y es el saldo de dos fuerzas que casi se cancelan:

- **Los meses nuevos (enero a abril de 2026) bajan el crecimiento potencial en 0,29 pp.** En esos
  meses la inflación básica sube de 5,0 % a 5,8 % con el desempleo estable. El modelo lo interpreta
  como un mercado laboral más estrecho de lo que se creía, sube la NAIRU de 2025 (8,61 % → 8,88 %) y
  con ello reduce el empleo potencial.
- **Las revisiones del DANE suben el crecimiento potencial en 0,21 pp.** Casi todo (0,16 pp) viene
  de la revisión del PIB. Esa revisión cambia el fechado del ciclo: aparece un pico en 2008, y eso
  modifica la tendencia de la productividad. Otros 0,05 pp vienen de la nueva desestacionalización
  de la tasa de desempleo.
- Los demás insumos (ingreso, GEIH trimestral, capital humano, inflación, tasa de cambio,
  expectativas, salarios, subempleo, vacantes) mueven alrededor de 0,003 pp en conjunto.

Por fuente: el bloque mensual explica −0,23 pp, el PIB +0,16 pp y el resto ≈ 0. La suma es −0,08
pp.

## 6. Notas para la tarjeta «Crecimiento del PIB potencial»

- Las tres líneas usan 100 × Δ4 log, que es la convención de `crecimiento_potencial` y de la
  descomposición. Con esa medida el paper da 3,28 / 2,54 / 2,68 / 1,96 en 2025, en vez de 3,33 /
  2,58 / 2,72 / 1,98. La brecha sigue en −0,07 a −0,09 pp.
- El PIB observado se mide igual que el potencial: anualizado, como suma de 4 trimestres. Así, la
  distancia entre la línea observada y la potencial es el cambio anual de la brecha del producto.
- Por las secciones 3.3 y 3.4, la diferencia con la línea punteada no se debe solo a revisiones del
  DANE. La descripción de la tarjeta dice «revisiones de datos del DANE y los meses nuevos», igual
  que la nota de cifras del paper que ya está en el tablero.
- La línea punteada se separa hasta 2 pp de la vigente en 2006–2008 por el pico de 2008T3. Ese
  efecto es real; no es un error de la gráfica.
