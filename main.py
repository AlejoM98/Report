import os
import json

from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel, json_to_piv

WORK_MODE = 'online'
PERIODOS   = ["day", "week", "month", "year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__), 'data', 'tags_data.json')

def main():
    # 1) Extraer datos y volcar JSON
    if WORK_MODE == 'online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) Leer JSON
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    # 3) Construir pivotes y generar reportes
    for p in PERIODOS:
        # daily
        df_daily = json_to_piv(raw[p]['daily'], index_field='Timestamp')
        generar_reporte_excel(df_daily, f"{p.capitalize()} Daily")
        # hourly
        df_hourly = json_to_piv(raw[p]['hourly'], index_field='Timestamp')
        generar_reporte_excel(df_hourly, f"{p.capitalize()} Hourly")

if __name__ == "__main__":
    main()
