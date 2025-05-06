import os, json
import pandas as pd

from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import abrir_writer, add_sheet, cerrar_writer

WORK_MODE = 'online'
PERIODOS   = ["day","week","month","year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__),'data','tags_data.json')
OUT_XLSX   = os.path.join(os.path.dirname(__file__),'data','reportes_por_planta.xlsx')

def build_name_map(raw_period):
    """
    A partir de los registros diarios+horarios de un periodo,
    construye un dict {TagUID: TagName}.
    """
    name_map = {}
    for subkey in ('daily','hourly'):
        for rec in raw_period[subkey]:
            uid = rec.get('TagUID')
            tn  = rec.get('TagName')
            if uid and tn:
                name_map[uid] = tn
    return name_map

def main():
    # 1) extraer datos y volcar JSON
    if WORK_MODE == 'online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) leer JSON
    with open(JSON_PATH,'r',encoding='utf-8') as f:
        raw = json.load(f)

    # 3) abrir writer
    abrir_writer(OUT_XLSX)

    # 4) procesar cada periodo
    for p in PERIODOS:
        rec = raw[p]
        name_map = build_name_map(rec)

        # convertir a DataFrames
        df_d = pd.DataFrame(rec['daily'])
        df_h = pd.DataFrame(rec['hourly'])

        # DAILY
        if not df_d.empty:
            df_d['Timestamp'] = pd.to_datetime(df_d['Timestamp'])
            piv = (
                df_d
                .pivot_table(index='Timestamp', columns='TagUID', values='Value', aggfunc='mean')
                .rename(columns=name_map)
                .reset_index()
            )
            add_sheet(piv, f"{p.capitalize()} Daily")
        else:
            print(f"⚠️ '{p.capitalize()} Daily' no tiene datos → salto")

        # HOURLY
        if not df_h.empty:
            df_h['Timestamp'] = pd.to_datetime(df_h['Timestamp'])
            piv = (
                df_h
                .pivot_table(index='Timestamp', columns='TagUID', values='Value', aggfunc='mean')
                .rename(columns=name_map)
                .reset_index()
            )
            add_sheet(piv, f"{p.capitalize()} Hourly")
        else:
            print(f"⚠️ '{p.capitalize()} Hourly' no tiene datos → salto")

    # 5) cerrar writer
    cerrar_writer()
    print("✅ Reporte completo generado en", OUT_XLSX)

if __name__=="__main__":
    main()
