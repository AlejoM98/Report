import pandas as pd
from xlsxwriter.utility import xl_col_to_name

# tag_mapping.json sigue usándose para los nombres amigables de planta/cuenca
import os, json
TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__),'..','config','tag_mapping.json'),
    encoding='utf-8'
))

def generar_reporte_excel(writer, df: pd.DataFrame, sheet_name: str):
    """
    writer: ExcelWriter abierto
    df:     DataFrame ya pivotado (primera columna Timestamp)
    sheet_name: nombre de la hoja (<=31 chars)
    """
    safe = sheet_name[:31]
    ws = writer.book.add_worksheet(safe)
    writer.sheets[safe] = ws

    # formatos
    header_fmt = writer.book.add_format({
        'align':'center','bold':True,'bg_color':'#D9D9D9','border':1
    })
    date_fmt = writer.book.add_format({'num_format':'yyyy-mm-dd','align':'center'})
    num_fmt  = writer.book.add_format({'num_format':'0.00'})
    red_fmt  = writer.book.add_format({'font_color':'#9C0006','bg_color':'#FFC7CE'})
    alt_fmt  = writer.book.add_format({'bg_color':'#F2F2F2'})

    # título
    last = xl_col_to_name(len(df.columns)-1)
    ws.merge_range(f'A1:{last}1', sheet_name, header_fmt)

    # escribir DF
    df.to_excel(writer, sheet_name=safe, startrow=2, index=False)

    # ajustar anchos y formatos
    for i, col in enumerate(df.columns):
        # ancho: max largo de contenido o nombre
        max_content = df[col].astype(str).map(len).max() if not df[col].empty else 0
        width = max(max_content, len(col)) + 2
        fmt = date_fmt if i==0 else num_fmt
        ws.set_column(i, i, width, fmt)

    # bandas alternas
    rows = len(df)
    if rows>0:
        ws.conditional_format(f'A3:{last}{3+rows}', {
            'type':'no_errors','format':alt_fmt,
            'criteria':'!=','value':''
        })
        # resaltar >100
        for col_idx in range(1, len(df.columns)):
            ws.conditional_format(3, col_idx, 3+rows, col_idx, {
                'type':'cell','criteria':'>','value':100,'format':red_fmt
            })

        # inserta un chart lineal en la derecha
        ch = writer.book.add_chart({'type':'line'})
        ch.add_series({
            'categories':[safe,2,0,2+rows-1,0],
            'values':    [safe,2,1,2+rows-1,1],
            'name':      sheet_name
        })
        ws.insert_chart(f'B{4+rows}', ch, {'x_scale':1.2,'y_scale':1.2})

    print(f"✅ Hoja '{sheet_name}' creada")
