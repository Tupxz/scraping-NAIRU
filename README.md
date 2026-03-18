# NAIRU Colombia – Data Pipeline

Repositorio para construir una base de datos reproducible del mercado laboral en Colombia con el objetivo de **estimar la NAIRU (Non-Accelerating Inflation Rate of Unemployment)** y recopilar estimaciones existentes publicadas por instituciones económicas.

El proyecto comienza con la construcción de un **pipeline de datos robusto y reproducible** para descargar, limpiar y almacenar series relevantes del mercado laboral, empezando por la **tasa de desempleo publicada por el DANE**.

---

# 1. Objetivo del repositorio

El objetivo de este proyecto es desarrollar una **infraestructura de datos reproducible** que permita:

1. Construir una base de datos limpia del mercado laboral colombiano.
2. Automatizar la descarga y actualización de datos relevantes.
3. Facilitar la estimación econométrica de la **NAIRU para Colombia**.
4. Documentar y comparar estimaciones de NAIRU publicadas por instituciones económicas.

El repositorio está diseñado como un **pipeline de datos modular**, de forma que pueda ampliarse posteriormente con nuevas fuentes y modelos econométricos.

---

# 2. Fuente principal actual

Actualmente el pipeline utiliza como fuente principal:

**Departamento Administrativo Nacional de Estadística (DANE)**

Serie utilizada:

- **Tasa de desempleo nacional**
- Fuente: Gran Encuesta Integrada de Hogares (GEIH)

Sitio oficial:

https://www.dane.gov.co

Esta serie constituye el **insumo base para futuras estimaciones de la NAIRU**.

En fases posteriores el repositorio incorporará:

- Inflación (IPC)
- Documentos de investigación del Banco de la República
- Informes del CARF
- Otras estimaciones institucionales de NAIRU

---

# 3. Estructura del proyecto
```
nairu-colombia/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│ ├── raw/
│ │ └── dane/
│ ├── interim/
│ └── processed/
├── logs/
├── src/
│ ├── init.py
│ ├── main.py
│ ├── dane.py
│ ├── io_utils.py
│ ├── quality_checks.py
│ └── config.py
├── notebooks/
│ └── exploration.ipynb
└── tests/
```

### Descripción de carpetas

**data/raw/**  
Datos descargados directamente de las fuentes sin transformación.

**data/interim/**  
Datos parcialmente procesados o normalizados.

**data/processed/**  
Base de datos final lista para análisis econométrico.

**logs/**  
Registros de ejecución del pipeline.

**src/**  
Código principal del pipeline.

**notebooks/**  
Exploración de datos y análisis preliminar.

**tests/**  
Pruebas unitarias del pipeline.

---

# 4. Cómo correr el pipeline

## 1. Clonar el repositorio

```bash
git clone https://github.com/usuario/nairu-colombia.git
cd nairu-colombia
```
## 2. Crear entorno virtual
En Mac

```bash
python -m venv venv
source venv/bin/activate
```

En Windows
```bash
venv\Scripts\activate
```
## 3. Instalar Dependencias
```bash
pip install -r requirements.txt
```

## 4. Ejecutar el pipeline
```bash
python src/main.py
```
- El pipeline realizará:

- Descarga de datos desde el DANE

- Limpieza y estandarización

- Validación de calidad

- Almacenamiento en data/processed/

# 5. Salida esperada

La ejecución del pipeline genera un dataset estructurado del mercado laboral.

Ejemplo de salida:
```
data/processed/unemployment_colombia.csv
```
Estructura esperada:

date	unemployment_rate	source	download_date
2020-01	13.1	DANE	2026-03-13
2020-02	12.4	DANE	2026-03-13
Campos:

- date → periodo de referencia

- unemployment_rate → tasa de desempleo (%)

- source → institución de origen

- download_date → fecha de descarga del pipeline

- Este dataset será utilizado posteriormente para:

estimación econométrica de la NAIRU

comparación con estimaciones institucionales

análisis del mercado laboral colombiano