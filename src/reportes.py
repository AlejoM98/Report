import os
import json
import pandas as pd

def cargar_datos(filename="tags_data.json"):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    filepath = os.path.join(data_dir, filename)
    try:
        with open(filepath, "r") as f:
            datos = json.load(f)
        return pd.DataFrame(datos)
    except Exception as e:
        print("Error al cargar datos:", e)
        return pd.DataFrame()

def generar_reportes():
    df = cargar_datos()
    if df.empty:
        print("No hay datos para generar reportes.")
        return
    
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    
    # Reporte diario
    reporte_diario = df.groupby(df["Timestamp"].dt.date).mean().reset_index()
    reporte_diario.to_csv(os.path.join(data_dir, "reporte_diario.csv"), index=False)
    
    # Reporte semanal
    reporte_semanal = df.groupby(df["Timestamp"].dt.to_period("W")).mean().reset_index()
    reporte_semanal.to_csv(os.path.join(data_dir, "reporte_semanal.csv"), index=False)
    
    # Reporte mensual
    reporte_mensual = df.groupby(df["Timestamp"].dt.to_period("M")).mean().reset_index()
    reporte_mensual.to_csv(os.path.join(data_dir, "reporte_mensual.csv"), index=False)
    
    # Reporte anual
    reporte_anual = df.groupby(df["Timestamp"].dt.to_period("Y")).mean().reset_index()
    reporte_anual.to_csv(os.path.join(data_dir, "reporte_anual.csv"), index=False)
    
    print("Reportes generados exitosamente.")

if __name__ == "__main__":
    generar_reportes()
