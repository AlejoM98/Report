import os
import json
import pandas as pd
from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day", "week", "month", "year"]
BASE_DIR   = os.path.dirname(__file__)
DATA_DIR   = os.path.join(BASE_DIR, 'data')
JSON_PATH  = os.path.join(DATA_DIR, 'tags_data.json')
OUT_PATH   = os.path.join(DATA_DIR, 'reportes_por_planta.xlsx')

def safe_pivot(df, index, columns, values):
    """Pivot seguro: si falta `values` o df está vacío, retorna df[[index]] vacío."""
    if df.empty or values not in df.columns:
        return pd.DataFrame({index: []})
    piv = df.pivot_table(index=index, columns=columns, values=values)
    return piv.reset_index()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Extraer datos y volcar a JSON
    if WORK_MODE == 'online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) Leer JSON
    with open(JSON_PATH,'r',encoding='utf-8') as f:
        raw = json.load(f)

    # 3) Eliminar Excel previo
    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    # 4) Un solo writer para todas las hojas
    with pd.ExcelWriter(OUT_PATH, engine='xlsxwriter') as writer:
        for p in PERIODOS:
            # Reconstruir DataFrames desde JSON
            df_d = pd.DataFrame(raw[p]['daily'])
            df_h = pd.DataFrame(raw[p]['hourly'])

            # Forzar datetime
            if 'Timestamp' in df_d:
                df_d['Timestamp'] = pd.to_datetime(df_d['Timestamp'])
            if 'Timestamp' in df_h:
                df_h['Timestamp'] = pd.to_datetime(df_h['Timestamp'])
                
            for df in (df_d, df_h):
                if 'Plant' not in df.columns:
                    df['Plant'] = ''
                if 'Basin' not in df.columns:
                    df['Basin'] = ''    

            # Pivot para Plants y Basins
            piv_plants_d = safe_pivot(df_d, 'Timestamp', 'Plant', 'Value')
            piv_basins_d = safe_pivot(df_d, 'Timestamp', 'Basin', 'Value')
            piv_plants_h = safe_pivot(df_h, 'Timestamp', 'Plant', 'Value')
            piv_basins_h = safe_pivot(df_h, 'Timestamp', 'Basin', 'Value')

            # Generar hojas
            generar_reporte_excel(piv_plants_d,  f"{p.capitalize()} Daily - Plants", writer)
            generar_reporte_excel(piv_basins_d,  f"{p.capitalize()} Daily - Basins",  writer)
            generar_reporte_excel(piv_plants_h, f"{p.capitalize()} Hourly - Plants", writer)
            generar_reporte_excel(piv_basins_h, f"{p.capitalize()} Hourly - Basins",  writer)

    print("✅ Reporte guardado en", OUT_PATH)

if __name__ == "__main__":
    main()
