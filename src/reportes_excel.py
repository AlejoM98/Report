import os, pandas as pd, json
from xlsxwriter.utility import xl_col_to_name

# Carga mapeo de plantas/cuencas
mp = os.path.join(os.path.dirname(__file__),'..','config','tag_mapping.json')
TAG_MAPPING = json.load(open(mp, encoding='utf-8'))

def generar_reporte_excel(pivot: pd.DataFrame, name_map: dict, sheet_name: str = "Daily"):
    """
    pivot: DataFrame pivotado con columnas = TagName
    name_map: dict {TagUID: TagName}
    """
    if pivot.empty:
        print(f"⚠️ '{sheet_name}' vacío, no creo hojas")
        return

    # Ruta de salida normalizada
    out_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__),'..','data','reportes_por_planta.xlsx')
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        wb = writer.book
        hdr_f = wb.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
        dt_f  = wb.add_format({'num_format':'yyyy-mm-dd','align':'center'})
        num_f = wb.add_format({'num_format':'0.00'})

        def crear_hoja(name, df_sub, title, chart_type='line'):
            safe = name[:31]
            ws   = wb.add_worksheet(safe)
            writer.sheets[safe] = ws

            last = xl_col_to_name(len(df_sub.columns)-1)
            ws.merge_range(f'A1:{last}1', title, hdr_f)
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
            ws.set_column(0,0,20,dt_f)
            ws.set_column(1,len(df_sub.columns)-1,15,num_f)

            if len(df_sub):
                ch = wb.add_chart({'type': chart_type})
                ch.add_series({
                    'categories':[safe,2,0,2+len(df_sub)-1,0],
                    'values':    [safe,2,1,2+len(df_sub)-1,1],
                    'name':      title
                })
                ws.insert_chart(f'B{4+len(df_sub)}', ch, {'x_scale':1.2,'y_scale':1.2})

        # 1) Hoja global
        crear_hoja(sheet_name, pivot.reset_index(), f"{sheet_name} Overview")

        # 2) Hojas por Planta: buscamos TagName de cada TagUID y lo extraemos del pivot
        for tuid, plant in TAG_MAPPING['plants'].items():
            tagname = name_map.get(tuid)
            if tagname and tagname in pivot.columns:
                df_sub = pivot[[tagname]].reset_index()
                crear_hoja(plant, df_sub, f"{sheet_name} - {plant}")

        # 3) Hojas por Cuenca
        for tuid, basin in TAG_MAPPING['basins'].items():
            tagname = name_map.get(tuid)
            if tagname and tagname in pivot.columns:
                df_sub = pivot[[tagname]].reset_index()
                crear_hoja(f"Cuenca {basin}", df_sub,
                           f"{sheet_name} - Cuenca {basin}", chart_type='column')

    print(f"📊 Reporte guardado en {out_path}")