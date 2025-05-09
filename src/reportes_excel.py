import pandas as pd
from xlsxwriter.utility import xl_col_to_name

def generar_reporte_excel(df: pd.DataFrame,
                          sheet_name: str,
                          writer,
                          add_chart: bool = False):
    """
    Inserta en `writer` una hoja con el DataFrame `df` + opcional gráfico.
    """
    if df.empty:
        print(f"⚠️ '{sheet_name}' no tiene datos, salto")
        return

    df = df.reset_index(drop=True)
    ncols = len(df.columns)
    safe = sheet_name[:31]
    ws = writer.book.add_worksheet(safe)
    writer.sheets[safe] = ws

    book       = writer.book
    header_fmt = book.add_format({
        'align':'center','bold':True,
        'bg_color':'#D9D9D9','border':1
    })
    date_fmt = book.add_format({
        'num_format':'yyyy-mm-dd hh:mm','align':'center'
    })
    num_fmt = book.add_format({'num_format':'0.00'})
    alt_fmt = book.add_format({'bg_color':'#F2F2F2'})

    # --- Título / header ---
    if ncols > 1:
        last = xl_col_to_name(ncols-1)
        ws.merge_range(f'A1:{last}1', sheet_name, header_fmt)
    else:
        ws.write(0, 0, sheet_name, header_fmt)

    # --- Escribo la tabla a partir de fila 2 ---
    df.to_excel(writer, sheet_name=safe, startrow=2, index=False)

    # --- Ajuste de columnas ---
    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(),
                      len(str(col)))
        width = max_len + 2
        fmt = date_fmt if idx == 0 else num_fmt
        ws.set_column(idx, idx, width, fmt)

    # --- Bandas alternas ---
    last_row = 2 + len(df)
    last_col = xl_col_to_name(ncols-1)
    ws.conditional_format(f'A3:{last_col}{last_row}', {
        'type':'no_errors','criteria':'!=','value':'','format':alt_fmt
    })

    # --- Gráfico de tendencia (primer TagName) ---
    if add_chart and ncols > 2:
        chart = book.add_chart({'type':'line'})
        # series: usamos la primera columna de datos (col idx 1)
        chart.add_series({
            'name':       df.columns[1],
            'categories': [safe, 2, 0, last_row-1, 0],
            'values':     [safe, 2, 1, last_row-1, 1],
        })
        chart.set_title({'name': 'Tendencia de ' + df.columns[1]})
        chart.set_x_axis({'name': 'Timestamp'})
        chart.set_y_axis({'name': 'Value'})
        # Inserto debajo de la tabla
        ws.insert_chart(f'A{last_row+3}', chart,
                        {'x_offset': 0, 'y_offset': 10})

    print(f"✅ Hoja '{sheet_name}' creada")
