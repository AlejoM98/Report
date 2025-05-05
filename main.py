import os
import json
from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day","week","month","year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__),'data','tags_data.json')

def json_to_piv(records, index_field):
    import pandas as pd
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df[index_field] = pd.to_datetime(df[index_field])
    piv = df.pivot_table(index=index_field, columns='TagName', values='Value', aggfunc='mean')
    return piv.reset_index()

def main():
    # 1) online → extraer + volcar JSON
    if WORK_MODE=='online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) offline → leer JSON
    with open(JSON_PATH,'r',encoding='utf-8') as f:
        raw = json.load(f)
    resultados = {
        p: json_to_piv(raw[p]['daily'],  'Timestamp')
        for p in PERIODOS
    }
    # Hourly solo para 'day'
    resultados.update({
        'day_hourly': json_to_piv(raw['day']['hourly'], 'Timestamp')
    })

    # 3) Generar Excel
    abrir_writer()
    for p, df in resultados.items():
        add_hojas(df, p.replace('_',' ').title())
    cerrar_writer()

if __name__ == "__main__":
    main()
