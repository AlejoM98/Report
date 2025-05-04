import os, pandas as pd, json
from xlsxwriter.utility import xl_col_to_name

# carga tag_mapping para encabezados
mp = os.path.join(os.path.dirname(__file__),'..','config','tag_mapping.json')
TAG_MAPPING = json.load(open(mp,encoding='utf-8'))

def generar_reporte_excel(df_raw, sheet_name="Daily"):
    if df_raw.empty:
        print(f"⚠️ '{sheet_name}' vacío, no creo hoja")
        return

    # pivot para TagName
    piv = df_raw.pivot_table(
        index='Date', columns='TagName', values='Value', aggfunc='mean'
    ).reset_index()

    out = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '..','data','reportes_por_planta.xlsx'))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
        wb = writer.book
        header = wb.add_format({'align':'center','bold':True,'bg_color':'#D9D9D9','border':1})
        datefm = wb.add_format({'num_format':'yyyy-mm-dd','align':'center'})
        numfm  = wb.add_format({'num_format':'0.00'})

        def crea_hoja(name, sub, title, ctype='line'):
            sn = name[:31]
            ws = wb.add_worksheet(sn)
            writer.sheets[sn] = ws
            last = xl_col_to_name(len(sub.columns)-1)
            ws.merge_range(f'A1:{last}1', title, header)
            # logos a izquierda y derecha
            ws.set_header('&L&G&R&G',
                          {'image_left': os.path.abspath('assets/LogoEpsas.jpg'),
                           'image_right':os.path.abspath('assets/LogoTechlogic.jpg'),
                           'image_left_position':1,'image_right_position':1})
            sub.to_excel(writer, sheet_name=sn, startrow=2, index=False)
            ws.set_column(0,0,20,datefm)
            ws.set_column(1,len(sub.columns)-1,15,numfm)
            if len(sub):
                ch = wb.add_chart({'type':ctype})
                ch.add_series({
                  'categories':[sn,2,0,2+len(sub)-1,0],
                  'values':    [sn,2,1,2+len(sub)-1,1],
                  'name':      title
                })
                ws.insert_chart(f'B{4+len(sub)}', ch, {'x_scale':1.2,'y_scale':1.2})

        # hoja global
        crea_hoja(sheet_name, piv, f"{sheet_name} Overview")
        # por planta
        for uid,plant in TAG_MAPPING['plants'].items():
            cols = [c for c in piv.columns if c=='Date' or c.endswith(f"_{uid}")]
            if len(cols)>1:
                crea_hoja(plant, piv[cols], f"{sheet_name} - {plant}")
        # por cuenca
        for uid,basin in TAG_MAPPING['basins'].items():
            cols = [c for c in piv.columns if c=='Date' or c.endswith(f"_{uid}")]
            if len(cols)>1:
                crea_hoja(f"Cuenca {basin}", piv[cols], f"{sheet_name} - Cuenca {basin}", ctype='column')

    print("📊 Reporte guardado en", out)
