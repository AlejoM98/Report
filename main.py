import os, json, pandas as pd
from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day","week","month","year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__),'data','tags_data.json')
OUT_PATH   = os.path.join(os.path.dirname(__file__),'data','reportes_por_planta.xlsx')

def main():
    # 1) Extraer y JSON
    if WORK_MODE=='online':
        resultados = {p: extraer_datos(p) for p in PERIODOS}
        guardar_json(resultados)

    # 2) Leer JSON
    with open(JSON_PATH,'r',encoding='utf-8') as f:
        raw = json.load(f)

    # 3) Crear un solo ExcelWriter y generar todas las hojas
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with pd.ExcelWriter(OUT_PATH, engine='xlsxwriter') as writer:
        for p in PERIODOS:
            # reconstruye df desde JSON
            df_d = pd.DataFrame(raw[p]['daily'])
            df_h = pd.DataFrame(raw[p]['hourly'])

            if not df_d.empty:
                piv_plants = df_d.pivot_table(
                    index='Timestamp', columns='Plant', values='Value'
                ).reset_index()
                generar_reporte_excel(writer, piv_plants, f"{p.capitalize()} Daily - Plants")

                piv_basins = df_d.pivot_table(
                    index='Timestamp', columns='Basin', values='Value'
                ).reset_index()
                generar_reporte_excel(writer, piv_basins, f"{p.capitalize()} Daily - Basins")

            if not df_h.empty:
                piv_plants_h = df_h.pivot_table(
                    index='Timestamp', columns='Plant', values='Value'
                ).reset_index()
                generar_reporte_excel(writer, piv_plants_h, f"{p.capitalize()} Hourly - Plants")

                piv_basins_h = df_h.pivot_table(
                    index='Timestamp', columns='Basin', values='Value'
                ).reset_index()
                generar_reporte_excel(writer, piv_basins_h, f"{p.capitalize()} Hourly - Basins")

    print("📊 Reporte guardado en", OUT_PATH)

if __name__=="__main__":
    main()
