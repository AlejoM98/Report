import os, json, pandas as pd
from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day","week","month","year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__),'data','tags_data.json')

def main():
    # extraer y JSON
    if WORK_MODE=='online':
        res = {p: extraer_datos(period=p) for p in PERIODOS}
        guardar_json(res)
    else:
        res = json.load(open(JSON_PATH,encoding='utf-8'))

    # generar reportes
    for p in PERIODOS:
        df_d = res[p]['daily']['raw']
        generar_reporte_excel(df_d, sheet_name=f"{p.capitalize()} Daily")
        df_h = res[p]['hourly']['raw']
        generar_reporte_excel(df_h, sheet_name=f"{p.capitalize()} Hourly")

if __name__=="__main__":
    main()
