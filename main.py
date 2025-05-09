import os
import json
import pandas as pd
from src.conexion import extraer_datos, guardar_json, TAG_MAPPING
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
    return piv.reset_index() if index in piv.index.names else piv

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1) Extraer + JSON
    if WORK_MODE == 'online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) Leer JSON
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    # 3) Borrar viejo Excel
    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    # 4) Generar nuevo
    with pd.ExcelWriter(OUT_PATH, engine='xlsxwriter') as writer:
        for periodo in PERIODOS:
            df_d = pd.DataFrame(raw[periodo]['daily'])
            df_h = pd.DataFrame(raw[periodo]['hourly'])

            # Asegurar datetime
            for df in (df_d, df_h):
                if 'Timestamp' in df:
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

            # Para cada tipo de agrupación...
            for kind, mapping in (("Plant", TAG_MAPPING['plants']),
                                  ("Basin", TAG_MAPPING['basins'])):
                # 1) Solo los nombres que realmente aparecen en daily
                nombres = sorted(df_d[kind].dropna().unique()) if kind in df_d else []

                for name in nombres:
                    # DAILY
                    sub_d = df_d[df_d[kind] == name]
                    piv_d = safe_pivot(sub_d, 'Timestamp', 'TagName', 'Value')
                    generar_reporte_excel(
                        piv_d,
                        f"{periodo.capitalize()} Daily - {name}",
                        writer,
                        add_chart=True
                    )

                    # HOURLY (solo si existe la columna)
                    if kind in df_h:
                        sub_h = df_h[df_h[kind] == name]
                    else:
                        sub_h = pd.DataFrame(columns=df_h.columns)
                    piv_h = safe_pivot(sub_h, 'Timestamp', 'TagName', 'Value')
                    generar_reporte_excel(
                        piv_h,
                        f"{periodo.capitalize()} Hourly - {name}",
                        writer,
                        add_chart=True
                    )

    print("✅ Reporte guardado en", OUT_PATH)

if __name__ == "__main__":
    main()
