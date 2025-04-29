import os
import pandas as pd

def generar_tabla_dinamica():
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    reporte_diario_path = os.path.join(data_dir, "reporte_diario.xlsx")
    
    try:
        df = pd.read_csv(reporte_diario_path)
    except Exception as e:
        print("❌ Error al cargar reporte diario:", e)
        return
    
    pivot = df.pivot_table(values="Value", index="TagName", columns="Timestamp", aggfunc="mean")
    
    archivo_excel = os.path.join(data_dir, "tabla_dinamica.xlsx")
    with pd.ExcelWriter(archivo_excel) as writer:
        pivot.to_excel(writer, sheet_name="Resumen")
    
    print(f"📊 Tabla dinámica generada en {archivo_excel}")

if __name__ == "__main__":
    generar_tabla_dinamica()
