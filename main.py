import os, json, pandas as pd
import src.conexion as conexion
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day","week","month","year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__),'data','tags_data.json')

def main():
    # 1) Extraer y guardar JSON
    if WORK_MODE == 'online':
        resultados = {p: conexion.extraer_datos(period=p) for p in PERIODOS}
        conexion.guardar_json(resultados)

    # 2) Leer JSON y generar pivotes + reportes
    raw_js = json.load(open(JSON_PATH, encoding='utf-8'))
    for p in PERIODOS:
        # Diarios
        raw_d = pd.DataFrame(raw_js[p]['daily'])
        if not raw_d.empty:
            raw_d['Date'] = pd.to_datetime(raw_d['Timestamp'])
            piv_d = raw_d.pivot_table(index='Date', columns='TagName', values='Value', aggfunc='mean')
            generar_reporte_excel(piv_d, raw_d, sheet_name=f"{p.capitalize()} Daily")

        # Horarios
        raw_h = pd.DataFrame(raw_js[p]['hourly'])
        if not raw_h.empty:
            raw_h['Date'] = pd.to_datetime(raw_h['Timestamp'])
            piv_h = raw_h.pivot_table(index='Date', columns='TagName', values='Value', aggfunc='mean')
            generar_reporte_excel(piv_h, raw_h, sheet_name=f"{p.capitalize()} Hourly")

if __name__ == "__main__":
    main()
