import os
import json
import time
import logging
import configparser
import pyodbc
import pandas as pd
from datetime import datetime, timedelta, timezone

# Carga tu mapeo de prefijos → nombre de planta/cuenca
TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping.json'),
    encoding='utf-8'
))

logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'app.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def leer_config():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
    return cfg

def conectar_bd(retries=3, backoff=1.5):
    cfg      = leer_config()
    db       = cfg['DATABASE']
    driver   = db['driver']
    server   = db['server']
    database = db['database']
    auth     = db.get('auth_mode','windows').lower()

    if auth == 'sql':
        uid, pwd = db['username'], db['password']
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"
    else:
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

    for i in range(retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            logging.info("✅ Conexión ODBC exitosa (intento %d)", i+1)
            return conn
        except Exception as e:
            logging.warning("❌ Error ODBC intento %d: %s", i+1, e)
            time.sleep(backoff ** i)
    raise ConnectionError("No se pudo conectar a la base de datos tras varios intentos")

def get_date_range(period="day"):
    now   = datetime.now(timezone.utc)
    today = datetime(now.year, now.month, now.day)
    if period=="day":
        return today, today + timedelta(days=1)
    if period=="week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=7)
    if period=="month":
        start = today.replace(day=1)
        nxt   = (start.replace(month=start.month%12+1, day=1)
                 if start.month<12 else start.replace(year=start.year+1, month=1, day=1))
        return start, nxt
    if period=="year":
        start = today.replace(month=1, day=1)
        return start, start.replace(year=start.year+1)
    raise ValueError("Periodo no soportado")

def extraer_datos(period="day"):
    conn       = conectar_bd()
    start, end = get_date_range(period)
    s0, e0     = start.isoformat(), end.isoformat()

    # 1) TagUID → TagName desde la vista VTagBrowsing
    q_tags = """
      SELECT DISTINCT
        TagUID,
        Tagname AS TagName
      FROM [IS].[VTagBrowsing];
    """
    df_tags = pd.read_sql(q_tags, conn)
    name_map = dict(zip(df_tags.TagUID, df_tags.TagName))

    # 2) Extraer valores RAW y luego agregados hourly si toca
    if period=="day":
        q_raw = """
          SELECT 
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS datetime2(0)) AS Date,
            TagUID,
            CASE WHEN Agg_NUM=0 THEN NULL
                 ELSE Agg_SUM/CAST(Agg_NUM AS float) END AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp >= ? AND TimeStamp < ?;
        """
        df       = pd.read_sql(q_raw, conn, params=[s0, e0])

        q_hour = """
          SELECT 
            DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp >= ? AND TimeStamp < ?
          GROUP BY DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0), TagUID;
        """
        df_hourly = pd.read_sql(q_hour, conn, params=[s0, e0])

    else:
        q_raw = """
          SELECT 
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp >= ? AND TimeStamp < ?
          GROUP BY CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date), TagUID;
        """
        df        = pd.read_sql(q_raw, conn, params=[s0, e0])
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    conn.close()

    # 3) Asegurar datetime
    if not df.empty:
        df['Date'] = pd.to_datetime(df['Date'])
    if not df_hourly.empty:
        df_hourly['Date'] = pd.to_datetime(df_hourly['Date'])

    # 4) Enriquecer con TagName + inferencia Plant/Basin
    for d in (df, df_hourly):
        if d.empty:
            continue

        d['TagName']  = d['TagUID'].map(name_map).fillna('Sin Nombre')
        d['Timestamp'] = d['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        # split por "_" y añadir "_" para casar con tus prefijos
        parts = d['TagName'].str.split(pat='_', n=1, expand=True)
        codes = parts[0] + '_'
        d['Plant'] = codes.map(TAG_MAPPING['plants']).fillna('')
        d['Basin'] = codes.map(TAG_MAPPING['basins']).fillna('')

        d.drop(columns=['Date'], inplace=True)

    return {'daily': df, 'hourly': df_hourly}

def guardar_json(resultados, filename='tags_data.json'):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    out = {}
    for periodo, dic in resultados.items():
        out[periodo] = {}
        for subkey, df in dic.items():
            raw = df.to_dict(orient='records')
            clean = []
            for rec in raw:
                base = {
                    'Value': rec['Value'],
                    'TagName': rec['TagName'],
                    'Timestamp': rec['Timestamp']
                }
                if rec.get('Plant'):
                    base['Plant'] = rec['Plant']
                elif rec.get('Basin'):
                    base['Basin'] = rec['Basin']
                clean.append(base)
            out[periodo][subkey] = clean

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    logging.info("✅ JSON limpio guardado en %s", path)
