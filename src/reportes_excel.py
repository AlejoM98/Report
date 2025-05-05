import os
import json
import pandas as pd
from xlsxwriter.utility import xl_col_to_name

# Carga el mapping de plantas y cuencas
TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping.json'),
    encoding='utf-8'
))

def generar_reporte_excel(df: pd.DataFrame, sheet_name: str):
    """
    Genera un archivo Excel con:
      - Una pestaña overview para df completo
      - Una pestaña por cada planta con sus tags
      - Una pestaña por cada cuenca con sus tags
    """
    if df.empty:
        print(f"⚠️ '{sheet_name}' vacío, no creo hoja")
        return

    # Asegura que la fecha sea columna normal
    df = df.reset_index()
    date_col = df.columns[0]

    # Ruta de salida
    out_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'data', 'reportes_por_planta.xlsx')
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Crea el ExcelWriter
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
    book   = writer.book

    # Formatos comunes
    header_fmt = book.add_format({
        'align':'center','bold':True,'bg_color':'#D9D9D9','border':1
    })
    date_fmt = book.add_format({'num_format':'yyyy-mm-dd','align':'center'})
    num_fmt  = book.add_format({'num_format':'0.00'})
    red_fmt  = book.add_format({'font_color':'#9C0006','bg_color':'#FFC7CE'})
    alt_fmt  = book.add_format({'bg_color':'#F2F2F2'})

    def _add_sheet(name, subdf, title, chart_type='line'):
        safe = name[:31]
        ws = book.add_worksheet(safe)
        writer.sheets[safe] = ws

        # Título unificado
        last = xl_col_to_name(len(subdf.columns)-1)
        ws.merge_range(f'A1:{last}1', title, header_fmt)

        # Escribe la tabla
        subdf.to_excel(writer, sheet_name=safe, startrow=2, index=False)

        # Ajusta anchos y formatos
        for i, col in enumerate(subdf.columns):
            width = max(subdf[col].astype(str).map(len).max(), len(col)) + 2
            fmt = date_fmt if i==0 else num_fmt
            ws.set_column(i, i, width, fmt)

        # Bandas alternas
        rows = len(subdf)
        ws.conditional_format(f'A3:{last}{3+rows}', {
            'type':'no_errors','format':alt_fmt,
            'criteria':'!=','value':''})

        # Ejemplo de condicional (valores >100 en rojo)
        for col_idx in range(1, len(subdf.columns)):
            ws.conditional_format(3, col_idx, 3+rows, col_idx, {
                'type':'cell','criteria':'>','value':100,'format':red_fmt
            })

        # Gráfico
        if rows>0:
            ch = book.add_chart({'type':chart_type})
            ch.add_series({
                'categories': [safe, 2, 0, 2+rows-1, 0],
                'values':     [safe, 2, 1, 2+rows-1, 1],
                'name':       title
            })
            ws.insert_chart(f'B{4+rows}', ch, {'x_scale':1.2,'y_scale':1.2})

    # --- Overview completo ---
    _add_sheet(sheet_name, df, f"{sheet_name} Overview")

    # --- Pestañas por planta ---
    for tag_uid, plant in TAG_MAPPING['plants'].items():
        # Busca columnas que terminan en _<tag_uid>
        cols = [c for c in df.columns if c!=date_col and c.endswith(f"_{tag_uid}")]
        if cols:
            sub = df[[date_col] + cols]
            _add_sheet(plant, sub, f"{sheet_name} - {plant}")

    # --- Pestañas por cuenca ---
    for tag_uid, basin in TAG_MAPPING['basins'].items():
        cols = [c for c in df.columns if c!=date_col and c.endswith(f"_{tag_uid}")]
        if cols:
            sub = df[[date_col] + cols]
            _add_sheet(f"Cuenca {basin}", sub, f"{sheet_name} - Cuenca {basin}", chart_type='column')

    # Cierra y guarda
    writer.close()
    print("📊 Reporte guardado en", out_path)
