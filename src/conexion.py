import configparser
import pyodbc
import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone

def get_date_range(period: str = "day"):
    now = datetime.now(timezone.utc)
    today = datetime(now.year, now.month, now.day)
    if period == "day":
        return today, today + timedelta(days=1)
    if period == "week":
        monday = today - timedelta(days=today.weekday())
        return monday, monday + timedelta(days=7)
    if period == "month":
        start = today.replace(day=1)
        end = start.replace(
            month=start.month % 12 + 1,
            year=start.year + (1 if start.month == 12 else 0)
        )
        return start, end
    if period == "year":
        start = today.replace(month=1, day=1)
        return start, start.replace(year=start.year + 1)
    raise ValueError(f"Periodo no soportado: {period}")
  

def leer_config():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
    return cfg


def conectar_bd():
    cfg = leer_config()
    server = cfg['DATABASE']['server']
    database = cfg['DATABASE']['database']
    driver = cfg['DATABASE']['driver']
    auth = cfg['DATABASE'].get('auth_mode', 'windows').lower()

    if auth == 'windows':
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        user = cfg['DATABASE']['username']
        pwd = cfg['DATABASE']['password']
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={pwd};"

    try:
        conn = pyodbc.connect(conn_str)
        print("✅ Conexión exitosa")
        return conn
    except Exception as e:
        print("❌ Error al conectar:", e)
        return None


def extraer_datos(period='day'):
    conn = conectar_bd()
    if not conn:
        return {}

    # 1) Rango de fechas
    start, end = get_date_range(period)
    s0 = start.strftime("%Y-%m-%dT%H:%M:%S")
    e0 = end.strftime("%Y-%m-%dT%H:%M:%S")

    # 2) TagUID → TagName
    q_map = f"""
      WITH LatestVersions AS (
        SELECT TV.TagUID, TV.TechnicalName,
               ROW_NUMBER() OVER (
                 PARTITION BY TV.TagUID ORDER BY TV.Created DESC
               ) AS rn
        FROM TLG.TagVersion TV
      )
      SELECT DISTINCT
        T.VTagUID AS TagUID,
        COALESCE(LV.TechnicalName,'Sin Nombre') AS TagName
      FROM TLG.Tag T
      LEFT JOIN LatestVersions LV
        ON LV.TagUID = T.TagUID AND LV.rn = 1;
    """
    df_map = pd.read_sql(q_map, conn)
    name_map = dict(zip(df_map['TagUID'], df_map['TagName']))

    # 3) TagUID → GroupName
    q_groups = '''
       SELECT VG.TagUID,
         COALESCE(VG.NameResourceText, 'Sin Grupo') AS GroupName  -- Asignar valor por defecto
        FROM TLG.VTagGroup_Texts VG
        WHERE VG.NameResource_LanguageID = 0;
    '''
    df_groups = pd.read_sql(q_groups, conn)
    tag_to_group = dict(zip(df_groups['TagUID'], df_groups['GroupName']))

    # 4) Cargar mapeo externo de plantas y cuencas
    mapping_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping_new.json')
    if not os.path.exists(mapping_path):
        raise FileNotFoundError(f"No existe el mapeo externo: {mapping_path}")
    with open(mapping_path, 'r', encoding='utf-8') as f:
        maps = json.load(f)
    # Cargar mapeos separados para plantas y cuencas
    plants_map = {taguid.lower().replace('-', '_'): plant for taguid, plant in maps["plants"].items()}
    basins_map = {taguid.lower().replace('-', '_'): basin for taguid, basin in maps["basins"].items()}

    # 5) Extraer datos según periodo
    if period == 'day':
        q_daily = f"""
          SELECT
            CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS datetime2) AS Date,
            AV.TagUID,
            CASE WHEN AV.Agg_NUM=0 THEN NULL
                 ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
            END AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
          ORDER BY AV.TimeStamp;
        """
        df_daily = pd.read_sql(q_daily, conn)

        q_hourly = f"""
          SELECT
            DATEADD(hour,
              DATEDIFF(hour, 0, SWITCHOFFSET(AV.TimeStamp,'+00:00')),0) AS Date,
            AV.TagUID,
            AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
          GROUP BY DATEADD(hour, DATEDIFF(hour, 0, SWITCHOFFSET(AV.TimeStamp,'+00:00')),0), AV.TagUID
          ORDER BY Date;
        """
        df_hourly = pd.read_sql(q_hourly, conn)

    elif period in ('week', 'month'):
        q_daily = f"""
          SELECT
            CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date) AS Date,
            AV.TagUID,
            AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
          GROUP BY CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date), AV.TagUID
          ORDER BY Date;
        """
        df_daily = pd.read_sql(q_daily, conn)
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    elif period == 'year':
        q_daily = f"""
          SELECT
            DATEFROMPARTS(YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')), MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')),1) AS Date,
            AV.TagUID,
            AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
          GROUP BY DATEFROMPARTS(YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')), MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')),1), AV.TagUID
          ORDER BY Date;
        """
        df_daily = pd.read_sql(q_daily, conn)
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    else:
        conn.close()
        raise ValueError(f"Periodo no soportado: {period}")

    # 6) Asignar TagName, Plant y Basin
    for df in (df_daily, df_hourly):
        if df.empty:
            continue
        
        df['TagName'] = df['TagUID'].map(name_map).fillna('Sin Nombre')
        
        df['GroupName'] = df['TagUID'].map(lambda x: tag_to_group.get(x, 'Sin Grupo'))   
        df['GroupKey'] = df['TagUID'].str.lower().str.replace('-', '_')
        
        # print("\n🔍 Mapeo TagUID → GroupKey → Plant/Basin:")
        # sample_data = df[['TagUID', 'GroupKey', 'Plant', 'Basin']].drop_duplicates().head(5) # Primeros 5 grupos no vacíos
        # for group in sample_groups:
        #     group_key = group.lower().replace(' ', '_').replace('-', '_')
        #     print(f" - GroupName: '{group}' → GroupKey: '{group_key}'")
        
        # Asignar Plant o Basin (excluyente) 
        df['Plant'] = df['GroupKey'].map(plants_map).fillna('')
        df['Basin'] = df['GroupKey'].map(basins_map).fillna('')
        
        print("\n🔍 Mapeo TagUID → GroupKey → Plant/Basin:")
        sample_data = df[['TagUID', 'GroupKey', 'Plant', 'Basin']].drop_duplicates().head(5)
        print(sample_data.to_string(index=False))
        
        #Validar grupos en ambas categorías
        unclassified = df[(df['Plant'] == '') & (df['Basin'] == '')]['GroupKey'].unique()
        if len(unclassified) > 0:
            print(f"⚠️ Grupos no clasificados: {unclassified}")
            
        #Validar ambas categorias
        conflict_groups = df[(df['Plant'] != '') & (df['Basin'] != '')]['GroupKey'].unique()
        if len(conflict_groups) > 0:
            print(f"❌ Grupos en plantas y cuencas: {conflict_groups}")
            raise ValueError("Conflicto en mapeo: Grupos presentes en plantas y cuencas")
        
        df.drop(columns=['GroupKey'], inplace=True)

    conn.close()

    # 7) Pivot para Excel
    pivot_daily = df_daily.pivot_table(index='Date', columns='TagName', values='Value', aggfunc='mean').reset_index()
    pivot_hourly = df_hourly.pivot_table(index='Date', columns='TagName', values='Value', aggfunc='mean').reset_index() if not df_hourly.empty else pd.DataFrame()

    return {'daily': {'raw': df_daily, 'pivot': pivot_daily}, 'hourly': {'raw': df_hourly, 'pivot': pivot_hourly}}


def guardar_json(resultados, filename='tags_data.json'):
    print("🔄 Iniciando guardar_json...")
    out = {}
    for period, data in resultados.items():
        print(f"  Procesando período '{period}' → registros diarios: {len(data['daily']['raw'])}, horarios: {len(data['hourly']['raw'])}")
        out[period] = {
            'daily': [
                {
                    'TagName': r['TagName'],
                    'Value':   r['Value'],
                    'Timestamp': r['Date'].isoformat(),
                    'Plant':   r['Plant'],
                    'Basin':   r['Basin'],
                } for _, r in data['daily']['raw'].iterrows()
            ],
            'hourly': [
                {
                    'TagName': r['TagName'],
                    'Value':   r['Value'],
                    'Timestamp': r['Date'].isoformat(),
                    'Plant':   r['Plant'],
                    'Basin':   r['Basin'],
                } for _, r in data['hourly']['raw'].iterrows()
            ]
        }

    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', filename))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2, default=str)
        print(f"✅ JSON guardado en {path}")
    except Exception as e:
        print(f"❌ Error al guardar JSON:", e)