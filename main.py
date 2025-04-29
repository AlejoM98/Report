from src.conexion import extraer_datos, guardar_json
from src.reportes_excel import generar_reporte_excel
import pandas as pd
import json, os

WORK_MODE = 'online'
PERIODOS   = ["day", "week", "month", "year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__), 'data', 'tags_data.json')

def main():
    # 1) Extraer en línea y volcar JSON
    if WORK_MODE == 'online':
        resultados_online = {p: extraer_datos(period=p) for p in PERIODOS}
        guardar_json(resultados_online)

    # 2) Leer JSON
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    resultados = {}
    for p in PERIODOS:
        df_d = pd.DataFrame(raw[p]["daily"])
        df_h = pd.DataFrame(raw[p]["hourly"])

        # convertir timestamps
        df_d["Date"] = pd.to_datetime(df_d["Timestamp"])
        if not df_h.empty and "Timestamp" in df_h.columns:
            df_h["HourStart"] = pd.to_datetime(df_h["Timestamp"])
        else:
            df_h["HourStart"] = pd.to_datetime([])

        # pivots
        piv_d = df_d.pivot_table(
            index="Date", columns="TagName", values="Value", aggfunc="mean"
        ).reset_index()

        if p == "day":
            piv_h = df_h.pivot_table(
                index="HourStart", columns="TagName", values="Value", aggfunc="mean"
            ).reset_index()
        else:
            piv_h = pd.DataFrame()

        resultados[p] = {"daily": piv_d, "hourly": piv_h}

    # 3) Generar reportes
    for p in PERIODOS:
        generar_reporte_excel(resultados[p]["daily"],
                              sheet_name=f"{p.capitalize()} Daily")
        generar_reporte_excel(resultados[p]["hourly"],
                              sheet_name=f"{p.capitalize()} Hourly")

if __name__ == "__main__":
    main()
