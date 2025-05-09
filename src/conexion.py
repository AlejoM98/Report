import os, json, time, logging, configparser, pyodbc, pandas as pd
from datetime import datetime, timedelta, timezone

# logger
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'app.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# carga mapeo
TAG_MAPPING = json.load(open(
    os.path.join(os.path.dirname(__file__), '..', 'config', 'tag_mapping.json'),
    encoding='utf-8'
))

def leer_config():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
    return cfg

def conectar_bd(retries=3, backoff=1.5):
    db = leer_config()['DATABASE']
    if db.get('auth_mode','windows').lower() == 'sql':
        conn_str = f"DRIVER={db['driver']};SERVER={db['server']};DATABASE={db['database']};UID={db['username']};PWD={db['password']}"
    else:
        conn_str = f"DRIVER={db['driver']};SERVER={db['server']};DATABASE={db['database']};Trusted_Connection=yes"
    for i in range(retries):
        try:
            c = pyodbc.connect(conn_str, timeout=5)
            logging.info("✅ Conexión ODBC exitosa (intento %d)", i+1)
            return c
        except Exception as e:
            logging.warning("❌ Error ODBC intento %d: %s", i+1, e)
            time.sleep(backoff ** i)
    raise ConnectionError("No se pudo conectar a la base de datos tras varios intentos")

def get_date_range(period="day"):
    now = datetime.now(timezone.utc)
    today = datetime(now.year, now.month, now.day)
    if period=="day":   return today, today + timedelta(days=1)
    if period=="week":  start = today - timedelta(days=today.weekday()); return start, start + timedelta(days=7)
    if period=="month": start = today.replace(day=1); nxt = (start.replace(month=start.month%12+1, day=1)
                            if start.month<12 else start.replace(year=start.year+1, month=1, day=1))
                        ; return start, nxt
    if period=="year":  start = today.replace(month=1, day=1); return start, start.replace(year=start.year+1)
    raise ValueError("Periodo no soportado")

def extraer_datos(period="day"):
    conn      = conectar_bd()
    s0, e0    = [dt.isoformat() for dt in get_date_range(period)]
    # 1) Traer TagUID → TagName desde vista
    q_tags = """
      SELECT DISTINCT TagUID, Tagname AS TagName
      FROM [IS].[VTagBrowsing];
    """
    df_tags = pd.read_sql(q_tags, conn)
    name_map = dict(zip(df_tags.TagUID, df_tags.TagName))

    # 2) Query raw / agregación
    if period=="day":
        q_raw = """
          SELECT
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS datetime2(0)) AS Date,
            TagUID,
            CASE WHEN Agg_NUM=0 THEN NULL ELSE Agg_SUM/CAST(Agg_NUM AS float) END AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp>=? AND TimeStamp<?;
        """
        df = pd.read_sql(q_raw, conn, params=[s0,e0])
        q_hour = """
          SELECT
            DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')), 0) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp>=? AND TimeStamp<? 
          GROUP BY DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0), TagUID;
        """
        df_hourly = pd.read_sql(q_hour, conn, params=[s0,e0])
    else:
        q_raw = """
          SELECT
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp>=? AND TimeStamp<? 
          GROUP BY CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date), TagUID;
        """
        df        = pd.read_sql(q_raw, conn, params=[s0,e0])
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    conn.close()

    # 3) Asegurar datetime
    for d in (df, df_hourly):
        if not d.empty:
            d['Date'] = pd.to_datetime(d['Date'])
            # TagName
            d['TagName']   = d['TagUID'].map(name_map).fillna('Sin Nombre')
            # Timestamp ISO
            d['Timestamp'] = d['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            # Plant/Basin por prefijo antes de "_"
            parts = d['TagName'].str.split('_', n=1, expand=True)
            d['Plant'] = parts[0].map(lambda c: TAG_MAPPING['plants'].get(c, "")).fillna("")
            d['Basin'] = parts[1].map(lambda c: TAG_MAPPING['basins'].get(c, "")).fillna("")
            d.drop(columns=['Date'], inplace=True)

    return {'daily': df, 'hourly': df_hourly}

def guardar_json(resultados, filename='tags_data.json'):
    path = os.path.join(os.path.dirname(__file__),'..','data', filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    out = {}
    for per, dic in resultados.items():
        out[per] = {}
        for key, df in dic.items():
            recs = df.to_dict(orient='records')
            clean = []
            for r in recs:
                base = {
                    "Value": r['Value'],
                    "TagName": r['TagName'],
                    "Timestamp": r['Timestamp']
                }
                if r.get('Plant'): base['Plant'] = r['Plant']
                elif r.get('Basin'): base['Basin'] = r['Basin']
                clean.append(base)
            out[per][key] = clean

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    logging.info("✅ JSON guardado en %s", path)
