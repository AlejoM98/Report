import os
import pandas as pd
import json
from xlsxwriter.utility import xl_col_to_name

TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__),'..','config','tag_mapping.json'),
    encoding='utf-8'
))

_writer = None
_book   = None

def abrir_writer(path):
    """Inicializa el ExcelWriter para toda la sesión."""
    global _writer, _book
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _writer = pd.ExcelWriter(path, engine='xlsxwriter')
    _book   = _writer.book

def add_sheet(df: pd.DataFrame, sheet_name: str):
    """
    Agrega una hoja normalizada: 
     - título
     - datos
     - autoancho de columnas
     - bandas alternas y condicional
     - gráfico
    """
    global _writer, _book
    if df.empty:
        print(f"⚠️ '{sheet_name}' vacío → no creo hoja")
        return

    # Formatos
    header_fmt = _book.add_format({
        'align':'center','bold':True,
        'bg_color':'#D9D9D9','border':1
    })
    date_fmt = _book.add_format({
        'num_format':'yyyy-mm-dd','align':'center'
    })
    num_fmt = _book.add_format({'num_format':'0.00'})
    red_fmt = _book.add_format({
        'font_color':'#9C0006','bg_color':'#FFC7CE'
    })
    alt_fmt = _book.add_format({'bg_color':'#F2F2F2'})

    # Preparo hoja
    safe = sheet_name[:31]
    ws   = _book.add_worksheet(safe)
    _writer.sheets[safe] = ws

    # Título
    last_col = xl_col_to_name(len(df.columns)-1)
    ws.merge_range(f'A1:{last_col}1', sheet_name, header_fmt)

    # Datos
    df.to_excel(_writer, sheet_name=safe, startrow=2, index=False)

    # Autoancho de columnas (ahora con max() de Python)
    for i, col in enumerate(df.columns):
        # Longitudes de cada celda transformada a string
        col_lens = df[col].astype(str).map(len).tolist()
        max_content = max(col_lens) if col_lens else 0
        width = max(max_content, len(col)) + 2
        fmt = date_fmt if i == 0 else num_fmt
        ws.set_column(i, i, width, fmt)

    # Bandas alternas
    rows = len(df)
    ws.conditional_format(f'A3:{last_col}{3+rows}', {
        'type':'no_errors','format':alt_fmt,
        'criteria':'!=','value':''
    })

    # Condicional (>100)
    for col_idx in range(1, len(df.columns)):
        ws.conditional_format(3, col_idx, 3+rows, col_idx, {
            'type':'cell','criteria':'>','value':100,'format':red_fmt
        })

    # Gráfico
    if rows:
        ch = _book.add_chart({'type':'line'})
        ch.add_series({
            'categories': [safe, 2, 0, 2+rows-1, 0],
            'values':     [safe, 2, 1, 2+rows-1, 1],
            'name':       sheet_name
        })
        ws.insert_chart(f'B{4+rows}', ch, {'x_scale':1.2,'y_scale':1.2})

    print(f"✅ Hoja '{sheet_name}' creada")

def cerrar_writer():
    """Cierra y guarda el archivo."""
    global _writer
    if _writer:
        _writer.close()
        print("📊 Reporte XLSX finalizado")
