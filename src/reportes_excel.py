import os, pandas as pd
from xlsxwriter.utility import xl_col_to_name

def generar_reporte_excel(df: pd.DataFrame, sheet_name: str, writer):
    if df.empty:
        print(f"⚠️ '{sheet_name}' vacío, salto")
        return

    book = writer.book
    ws   = book.add_worksheet(sheet_name[:31])
    writer.sheets[sheet_name[:31]] = ws

    # formatos
    hdr = book.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
    dtf = book.add_format({'num_format':'yyyy-mm-dd hh:mm','align':'center'})
    num = book.add_format({'num_format':'0.00'})
    alt = book.add_format({'bg_color':'#F2F2F2'})
    red = book.add_format({'font_color':'#9C0006','bg_color':'#FFC7CE'})

    # título
    last = xl_col_to_name(len(df.columns)-1)
    ws.merge_range(f'A1:{last}1', sheet_name, hdr)

    # escribir data
    df.to_excel(writer, startrow=2, index=False)

    # columns autosize & formats
    for i, col in enumerate(df.columns):
        width = max(df[col].astype(str).map(len).max(), len(col)) + 2
        fmt   = dtf if i==0 else num
        ws.set_column(i, i, width, fmt)

    # bandas alternas
    rows = len(df)
    ws.conditional_format(f'A3:{last}{3+rows}', {
        'type':'no_errors','format':alt,'criteria':'!=','value':''
    })
    # resaltar >100
    for c in range(1, len(df.columns)):
        ws.conditional_format(3, c, 3+rows, c, {
            'type':'cell','criteria':'>','value':100,'format':red
        })

    # gráfico
    if rows>0:
        ch = book.add_chart({'type':'line'})
        ch.add_series({
            'categories':[sheet_name[:31],2,0,2+rows-1,0],
            'values':    [sheet_name[:31],2,1,2+rows-1,1],
            'name':      sheet_name
        })
        ws.insert_chart(f'B{4+rows}', ch, {'x_scale':1.2,'y_scale':1.2})

    print("✅ Hoja '", sheet_name, "' creada")
