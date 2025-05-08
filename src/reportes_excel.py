import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name

def generar_reporte_excel(df: pd.DataFrame, sheet_name: str, writer: pd.ExcelWriter):
    """
    Crea una hoja `sheet_name` en `writer` a partir de df:
      - Formato cabecera
      - Alternancia de bandas
      - Condicional >100
      - Gráfico
    """
    if df.empty:
        print(f"⚠️ '{sheet_name}' no tiene datos → salto")
        return

    book  = writer.book
    ws    = book.add_worksheet(sheet_name[:31])
    writer.sheets[sheet_name[:31]] = ws

    # Formatos
    header_fmt = book.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
    date_fmt   = book.add_format({'num_format':'yyyy-mm-dd','align':'center'})
    num_fmt    = book.add_format({'num_format':'0.00'})
    red_fmt    = book.add_format({'font_color':'#9C0006','bg_color':'#FFC7CE'})
    alt_fmt    = book.add_format({'bg_color':'#F2F2F2'})

    # Título
    last = xl_col_to_name(len(df.columns)-1)
    ws.merge_range(f'A1:{last}1', sheet_name, header_fmt)

    # Escribir tabla
    df.to_excel(writer, sheet_name=sheet_name[:31], startrow=2, index=False)

    # Columnas auto-fit y formatos
    for i, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        fmt = date_fmt if i==0 else num_fmt
        ws.set_column(i, i, max_len, fmt)

    # Alternar bandas
    rows = len(df)
    ws.conditional_format(f'A3:{last}{3+rows}', {
        'type':'no_errors','format':alt_fmt,'criteria':'!=','value':''
    })
    # Condición >100
    for col_idx in range(1, len(df.columns)):
        ws.conditional_format(3, col_idx, 3+rows, col_idx, {
            'type':'cell','criteria':'>','value':100,'format':red_fmt
        })

    # Gráfico
    chart = book.add_chart({'type':'line'})
    chart.add_series({
        'categories': [sheet_name[:31], 2, 0, 2+rows-1, 0],
        'values':     [sheet_name[:31], 2, 1, 2+rows-1, 1],
        'name':       sheet_name
    })
    ws.insert_chart(f'B{4+rows}', chart, {'x_scale':1.2,'y_scale':1.2})

    print("✅ Hoja", repr(sheet_name), "creada")
