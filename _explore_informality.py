"""Script temporal: inspeccionar columnas de fecha en hoja Prop informalidad."""
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
import warnings
warnings.filterwarnings("ignore")

base_url = "https://www.dane.gov.co"
url = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "mercado-laboral/empleo-informal-y-seguridad-social"
)
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}
r = requests.get(url, headers=headers, timeout=30, verify=False)
soup = BeautifulSoup(r.text, "html.parser")
xlsx_links = [
    base_url + a["href"]
    for a in soup.find_all("a", href=True)
    if ".xlsx" in a["href"].lower() and "GEIHEISS" in a["href"]
]
print(f"URL: {xlsx_links[0]}")
r2 = requests.get(xlsx_links[0], headers=headers, timeout=60, verify=False)
xl = pd.ExcelFile(BytesIO(r2.content))

# Hoja Prop informalidad — ver todas las filas y primeras columnas
df = xl.parse("Prop informalidad", header=None)
print(f"\nShape: {df.shape}")
print(f"\n--- Primeras 20 columnas de filas 8-15 ---")
print(df.iloc[8:16, :20].to_string())
print(f"\n--- Últimas 10 columnas de fila 13 (13 ciudades) ---")
print(df.iloc[13, -10:].to_string())
print(f"\n--- Fila 10 completa (encabezado columnas) ---")
print(df.iloc[10, :30].to_string())
print(f"\n--- Fila 11 completa (fechas) ---")
print(df.iloc[11, :30].to_string())
