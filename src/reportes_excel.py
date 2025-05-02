import os, pandas as pd, json
from xlsxwriter.utility import xl_col_to_name

def generar_reporte_excel(pivot: pd.DataFrame,
                          raw: pd.DataFrame,
                          sheet_name: str = "Daily"):
    if pivot.empty:
        print(f"⚠️ '{sheet_name}' vacío, no creo hojas")
        return

    out_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),'..','data','reportes_por_planta.xlsx'
    ))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        wb = writer.book
        fmt_hdr = wb.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
        fmt_dt  = wb.add_format({'num_format':'yyyy-mm-dd','align':'center'})
        fmt_num = wb.add_format({'num_format':'0.00'})

        def crear_hoja(name, df_sub, title, chart_type='line'):
            safe = name[:31]
            ws   = wb.add_worksheet(safe)
            writer.sheets[safe] = ws
            last = xl_col_to_name(len(df_sub.columns)-1)
            ws.merge_range(f'A1:{last}1', title, fmt_hdr)
            ws.set_header(
                '&L&G&R&G',
                {
                  'image_left':  os.path.abspath('assets/LogoEpsas.jpg'),
                  'image_right': os.path.abspath('assets/LogoTechlogic.jpg'),
                  'image_left_position':  1,
                  'image_right_position': 1
                }
            )
            df_sub.to_excel(writer, sheet_name=safe, startrow=2, index=False)
            ws.set_column(0,0,20,fmt_dt)
            ws.set_column(1,len(df_sub.columns)-1,15,fmt_num)
            if len(df_sub):
                ch = wb.add_chart({'type':chart_type})
                ch.add_series({
                    'categories':[safe,2,0,2+len(df_sub)-1,0],
                    'values':    [safe,2,1,2+len(df_sub)-1,1],
                    'name':       title
                })
                ws.insert_chart(f'B{4+len(df_sub)}', ch, {'x_scale':1.2,'y_scale':1.2})

        # 1) Overview
        crear_hoja(sheet_name, pivot.reset_index(), f"{sheet_name} Overview")

        # 2) Por Planta
        for plant in raw['Plant'].unique():
            if not plant: continue
            tags = raw.loc[raw['Plant']==plant, 'TagName'].unique().tolist()
            cols = ['Date'] + [t for t in tags if t in pivot.columns]
            if len(cols)>1:
                crear_hoja(plant, pivot[cols].reset_index(), f"{sheet_name} - {plant}")

        # 3) Por Cuenca
        for basin in raw['Basin'].unique():
            if not basin: continue
            tags = raw.loc[raw['Basin']==basin, 'TagName'].unique().tolist()
            cols = ['Date'] + [t for t in tags if t in pivot.columns]
            if len(cols)>1:
                crear_hoja(f"Cuenca {basin}", pivot[cols].reset_index(),
                           f"{sheet_name} - Cuenca {basin}", chart_type='column')

    print("📊 Reporte guardado en", out_path)
