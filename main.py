import os, json, pandas as pd
import src.conexion as conexion
from src.reportes_excel import generar_reporte_excel

WORK_MODE = 'online'
PERIODOS   = ["day", "week", "month", "year"]
JSON_PATH  = os.path.join(os.path.dirname(__file__), 'data', 'tags_data.json')

def main():
    # 1) Extraer y guardar JSON
    if WORK_MODE == 'online':
        resultados_online = {p: conexion.extraer_datos(period=p) for p in PERIODOS}
        conexion.guardar_json(resultados_online)

    # 2) Construir name_map de TagUID→TagName
    if conexion.df_map is None:
        raise RuntimeError("df_map no inicializado; llama a extraer_datos primero.")
    name_map = dict(zip(conexion.df_map.TagUID, conexion.df_map.TagName))

    # 3) Leer JSON y generar pivotes/reportes
    raw = json.load(open(JSON_PATH, encoding='utf-8'))

    for p in PERIODOS:
        df_d = pd.DataFrame(raw[p]["daily"])
        df_h = pd.DataFrame(raw[p]["hourly"])

        # Timestamp → Date
        df_d["Date"] = pd.to_datetime(df_d["Timestamp"])
        df_h["Date"] = pd.to_datetime(df_h["Timestamp"]) if not df_h.empty else pd.Series(dtype='datetime64[ns]')

        # Pivot sobre TagName
        piv_d = df_d.pivot_table(index="Date", columns="TagName", values="Value", aggfunc="mean")
        piv_h = df_h.pivot_table(index="Date", columns="TagName", values="Value", aggfunc="mean") if not df_h.empty else pd.DataFrame()

        generar_reporte_excel(piv_d, name_map, sheet_name=f"{p.capitalize()} Daily")
        generar_reporte_excel(piv_h, name_map, sheet_name=f"{p.capitalize()} Hourly")

if __name__ == "__main__":
    main()
