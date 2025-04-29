import os
import pandas as pd
from datetime import datetime
from xlsxwriter.utility import xl_col_to_name
import json

# 1) Carga mapping planta/cuenca
mapping_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping.json')
with open(mapping_path, 'r', encoding='utf-8') as f:
    TAG_MAPPING = json.load(f)

def generar_reporte_excel(df: pd.DataFrame, sheet_name: str = "Daily"):
    """
    Crea 'reportes_por_planta.xlsx' con:
      1) Hoja global (sheet_name)
      2) Hojas separadas por planta
      3) Hojas separadas por cuenca

    Parámetros:
    - df: DataFrame PIVOTADO (tiene columna Date o HourStart y luego una columna
          por cada TagName con valores numéricos).
    - sheet_name: nombre de la hoja global.
    """
    # 2) Si no hay datos, salimos
    if df.empty:
        print(f"⚠️ DataFrame vacío, no se genera hoja '{sheet_name}'")
        return

    # 3) Detectar columna de fecha
    if 'Date' in df.columns:
        date_col = 'Date'
    elif 'HourStart' in df.columns:
        date_col = 'HourStart'
    else:
        raise ValueError(f"No se encontró columna de fecha en df para hoja '{sheet_name}'")

    # Asegurar datetime
    df[date_col] = pd.to_datetime(df[date_col])

    # 4) Preparar ExcelWriter
    output_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'reportes_por_planta.xlsx')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        wb = writer.book

        # Formatos
        header_fmt = wb.add_format({
            'align': 'center', 'bold': True,
            'bg_color': '#D9D9D9', 'border': 1
        })
        date_fmt = wb.add_format({'num_format': 'yyyy-mm-dd', 'align': 'center'})
        num_fmt  = wb.add_format({'num_format': '0.00'})

        def crear_hoja(name: str, pivot: pd.DataFrame, title: str, chart_type: str = 'line'):
            """Función interna para construir cada hoja de Excel."""
            safe_name = name[:31]  # límite de Excel
            ws = wb.add_worksheet(safe_name)
            writer.sheets[safe_name] = ws

            # Título fusionado
            last_col = xl_col_to_name(len(pivot.columns) - 1)
            ws.merge_range(f'A1:{last_col}1', title, header_fmt)
            
            ws.set_header('&L&G', {'image_left': 'assets/LogoEpsas.jpg', 'image_left_position': 1})
            ws.set_header('&L&G', {'image_rigth': 'assets/LogoTechlogic.jpg', 'image_rigth_position': 1})

            # Escribir tabla desde fila 3
            pivot.to_excel(writer, sheet_name=safe_name, startrow=2, index=False)

            # Formatear
            ws.set_column(0, 0, 20, date_fmt)  # columna de fecha
            ws.set_column(1, len(pivot.columns)-1, 15, num_fmt)

            # Insertar gráfico si hay datos
            rows = len(pivot)
            if rows > 0:
                chart = wb.add_chart({'type': chart_type})
                chart.add_series({
                    'categories': [safe_name, 2, 0, 2 + rows - 1, 0],
                    'values':     [safe_name, 2, 1, 2 + rows - 1, 1],
                    'name':       title
                })
                ws.insert_chart(f'B{4+rows}', chart, {'x_scale':1.2, 'y_scale':1.2})

        # 5) Hoja GLOBAL
        crear_hoja(sheet_name, df, f"{sheet_name} Overview", chart_type='line')

        # 6) Hojas por PLANTA
        for prefix, plant in TAG_MAPPING['plants'].items():
            # columnas que empiecen con ese prefijo
            cols = [date_col] + [
                c for c in df.columns
                if c != date_col and c.split('-',1)[0] == prefix
            ]
            if len(cols) > 1:
                sub = df[cols]
                crear_hoja(plant, sub, f"{sheet_name} - {plant}", chart_type='line')

        # 7) Hojas por CUENCA
        for prefix, basin in TAG_MAPPING['basins'].items():
            cols = [date_col] + [
                c for c in df.columns
                if c != date_col and c.split('-',1)[0] == prefix
            ]
            if len(cols) > 1:
                sub = df[cols]
                crear_hoja(f"Cuenca {basin}", sub,
                           f"{sheet_name} - Cuenca {basin}", chart_type='column')

    print(f"📊 Reporte guardado en {output_path}")
