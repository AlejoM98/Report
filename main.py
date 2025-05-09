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
    if df.empty or columns not in df.columns or values not in df.columns:
        return pd.DataFrame({index: []})
    piv = df.pivot_table(index=index, columns=columns, values=values)
    if index in piv.index.names:
        return piv.reset_index()
    return piv

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) online → extraer + JSON
    if WORK_MODE == 'online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) leer JSON
    raw = json.load(open(JSON_PATH,'r',encoding='utf-8'))

    # 3) borrar Excel viejo
    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    # 4) abrir un solo writer
    with pd.ExcelWriter(OUT_PATH, engine='xlsxwriter') as writer:
        for p in PERIODOS:
            df_d = pd.DataFrame(raw[p]['daily'])
            df_h = pd.DataFrame(raw[p]['hourly'])

            for df in (df_d, df_h):
                if 'Timestamp' in df:
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

            # pivote por Plant / Basin
            piv_plants_d = safe_pivot(df_d, 'Timestamp','Plant','Value')
            piv_basins_d = safe_pivot(df_d, 'Timestamp','Basin','Value')
            piv_plants_h = safe_pivot(df_h, 'Timestamp','Plant','Value')
            piv_basins_h = safe_pivot(df_h, 'Timestamp','Basin','Value')

            # crear hojas
            generar_reporte_excel(piv_plants_d,
                f"{p.capitalize()} Daily - Plants", writer)
            generar_reporte_excel(piv_basins_d,
                f"{p.capitalize()} Daily - Basins", writer)
            generar_reporte_excel(piv_plants_h,
                f"{p.capitalize()} Hourly - Plants", writer)
            generar_reporte_excel(piv_basins_h,
                f"{p.capitalize()} Hourly - Basins", writer)

    print("✅ Reporte guardado en", OUT_PATH)

if __name__ == "__main__":
    main()
