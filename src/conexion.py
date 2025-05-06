import os
import json
import time
import logging
import configparser
import pyodbc
import pandas as pd
from datetime import datetime, timedelta, timezone

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
    conn  = conectar_bd()
    start, end = get_date_range(period)
    s0, e0     = start.isoformat(), end.isoformat()

    # 1) TagUID → TagName
    q_map = """
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
        ON LV.TagUID=T.TagUID AND LV.rn=1;
    """
    df_map = pd.read_sql(q_map, conn)

    # 2) Relación TagUID → GroupName / Kind (plant|basin)
    q_rel = """
      DECLARE 
            @UID_PLANTA UNIQUEIDENTIFIER = '19CDF42E-1E2E-4DBF-0001-000000000001',
            @UID_CUENCA  UNIQUEIDENTIFIER = '19CDF42E-1E2E-4DBF-0001-000000000002';

      WITH LatestTag AS (
          SELECT
              TagUID,
              TechnicalName,
              ROW_NUMBER() OVER (PARTITION BY TagUID ORDER BY Created DESC) AS rn
          FROM TLG.TagVersion
          WHERE Deleted IS NULL
      )
      SELECT
          t.TagUID,
          COALESCE(tv.TechnicalName,'') AS TagName,
          COALESCE(lt.Text,'')           AS GroupName,
          CASE
            WHEN gv.GroupTypeUID = @UID_PLANTA THEN 'plant'
            WHEN gv.GroupTypeUID = @UID_CUENCA  THEN 'basin'
            ELSE 'other'
          END AS Kind
      FROM TLG.Tag AS t
      LEFT JOIN LatestTag AS tv
        ON t.TagUID = tv.TagUID AND tv.rn = 1
      LEFT JOIN TLG.Rel_Group_Tag AS rgt
        ON t.TagUID = rgt.TagUID
      LEFT JOIN COMMON.[Group] AS g
        ON rgt.GroupUID = g.GroupUID
      LEFT JOIN COMMON.GroupVersion AS gv
        ON g.GroupUID = gv.GroupUID AND gv.Deleted IS NULL
      OUTER APPLY COMMON.udf_GetUnpivotLanguageTexts(gv.NameResourceUID) AS lt
      WHERE lt.LanguageID = 0;
    """
    df_rel = pd.read_sql(q_rel, conn)

    # Eliminamos duplicados para que cada TagUID aparezca solo UNA vez
    df_rel_unique = df_rel.drop_duplicates(subset=['TagUID'], keep='first')
    # Ahora podemos convertirlo en mapa sin errores
    rel_map = df_rel_unique.set_index('TagUID')[['GroupName','Kind']].to_dict('index')

    # Construimos también el map de TagUID→TagName
    name_map = dict(zip(df_map.TagUID, df_map.TagName))

    # 3) Consultas de valores
    if period == 'day':
        q = """
          SELECT 
            CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS datetime2(0)) AS Date,
            AV.TagUID,
            CASE WHEN AV.Agg_NUM=0 THEN NULL
                 ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END         AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp>=? AND AV.TimeStamp<?;
        """
        df      = pd.read_sql(q, conn, params=[s0,e0])
        qh = """
          SELECT 
            DATEADD(hour,DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END)         AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp>=? AND TimeStamp<? 
          GROUP BY DATEADD(hour,DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0), TagUID;
        """
        
        df_hourly = pd.read_sql(qh, conn, params=[s0,e0])
    else:
        q = """
          SELECT 
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp>=? AND TimeStamp<? 
          GROUP BY CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date), TagUID;
        """
        df        = pd.read_sql(q, conn, params=[s0,e0])
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])
        
    if not df.empty:
        df['Date'] = pd.to_datetime(df['Date'])
    if not df_hourly.empty:
        df_hourly['Date'] = pd.to_datetime(df_hourly['Date'])

    conn.close()

    # 4) Enriquecer con nombres y grupos
    for d in (df, df_hourly):
        if d.empty: 
            continue
        d['TagName']   = d.TagUID.map(name_map).fillna('Sin Nombre')
        d['rawDate']   = d.Date
        d['Timestamp'] = d.Date.dt.strftime('%Y-%m-%dT%H:%M:%S')
        d['GroupName'] = d.TagUID.map(lambda u: rel_map.get(u,{}).get('GroupName',''))
        d['Kind']      = d.TagUID.map(lambda u: rel_map.get(u,{}).get('Kind',''))
        d.drop(columns=['Date'], inplace=True)

    return {'daily': df, 'hourly': df_hourly}

def guardar_json(resultados, filename='tags_data.json'):
    import os, json, logging
    path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    out = {}
    # resultados: { periodo: {'daily': df1, 'hourly': df2} }
    for periodo, dic in resultados.items():
        out[periodo] = {}
        for subkey, df in dic.items():  # subkey es 'daily' o 'hourly'
            # convertir cada DataFrame a lista de registros
            out[periodo][subkey] = df.to_dict(orient='records')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    logging.info("✅ JSON guardado en %s", path)
