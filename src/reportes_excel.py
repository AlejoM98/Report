import os
import json
import pandas as pd
from xlsxwriter.utility import xl_col_to_name

TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping.json'),
    encoding='utf-8'
))

def json_to_piv(records, index_field='Timestamp'):
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df[index_field] = pd.to_datetime(df[index_field])
    piv = df.pivot_table(
        index=index_field,
        columns='TagName',
        values='Value',
        aggfunc='mean'
    ).reset_index()
    return piv

def generar_reporte_excel(df: pd.DataFrame, sheet_name: str):
    if df.empty:
        print(f"⚠️ '{sheet_name}' vacío, no creo hoja")
        return

    date_col = df.columns[0]
    out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'reportes_por_planta.xlsx')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        book = writer.book
        header_fmt = book.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
        date_fmt   = book.add_format({'num_format':'yyyy-mm-dd','align':'center'})
        num_fmt    = book.add_format({'num_format':'0.00'})
        
        def _add_sheet(name, subdf, title, chart_type='line'):
            safe = name[:31]
            ws = book.add_worksheet(safe)
            writer.sheets[safe] = ws
            last = xl_col_to_name(len(subdf.columns)-1)
            ws.merge_range(f'A1:{last}1', title, header_fmt)
            subdf.to_excel(writer, sheet_name=safe, startrow=2, index=False)
            ws.set_column(0, 0, 20, date_fmt)
            ws.set_column(1, len(subdf.columns)-1, 15, num_fmt)
            if len(subdf)>0:
                ch = book.add_chart({'type':chart_type})
                ch.add_series({
                    'categories': [safe,2,0,2+len(subdf)-1,0],
                    'values':     [safe,2,1,2+len(subdf)-1,1],
                    'name':       title
                })
                ws.insert_chart(f'B{4+len(subdf)}', ch, {'x_scale':1.2,'y_scale':1.2})

        # Overview
        _add_sheet(sheet_name, df, f"{sheet_name} Overview")

        # Por planta
        for tuid, plant in TAG_MAPPING['plants'].items():
            cols = [c for c in df.columns if c!=date_col and c.endswith(f"_{tuid}")]
            if cols:
                sub = df[[date_col]+cols]
                _add_sheet(plant, sub, f"{sheet_name} - {plant}")

        # Por cuenca
        for tuid, basin in TAG_MAPPING['basins'].items():
            cols = [c for c in df.columns if c!=date_col and c.endswith(f"_{tuid}")]
            if cols:
                sub = df[[date_col]+cols]
                _add_sheet(f"Cuenca {basin}", sub, f"{sheet_name} - Cuenca {basin}", chart_type='column')

    print("📊 Reporte guardado en", out_path)
