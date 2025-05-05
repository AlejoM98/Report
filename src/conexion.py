import os
import json
import time
import logging
import configparser
import pyodbc
import pandas as pd
from datetime import datetime, timedelta, timezone

# --- Logging ---
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__),'..','app.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# --- Leer config.ini ---
cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.path.dirname(__file__),'..','config','config.ini'))

# --- Sección DATABASE ---
db = cfg['DATABASE']
DRIVER     = db['driver']
SERVER     = db['server']
DATABASE   = db['database']
AUTH       = db.get('auth_mode','windows').lower()
UID_PLANTA = db.get('uid_planta')
UID_CUENCA = db.get('uid_cuenca')
USER       = db.get('username','')
PWD        = db.get('password','')

def conectar_bd(retries=3, backoff=2.0):
    if AUTH == 'windows':
        conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    else:
        conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PWD};"
    for i in range(retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            logging.info("✅ Conexión BD exitosa (intento %d)", i+1)
            return conn
        except Exception as e:
            logging.warning("❌ intento %d fallido: %s", i+1, e)
            time.sleep(backoff**i)
    logging.critical("No pudo conectar BD tras %d intentos", retries)
    raise ConnectionError("No se pudo conectar a la BD")

def get_date_range(period="day"):
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0,minute=0,second=0,microsecond=0)
    if period=="day":
        return today, today + timedelta(days=1)
    if period=="week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=7)
    if period=="month":
        start = today.replace(day=1)
        nxt = (start + timedelta(days=32)).replace(day=1)
        return start, nxt
    if period=="year":
        start = today.replace(month=1,day=1)
        return start, start.replace(year=start.year+1)
    raise ValueError(f"Periodo desconocido: {period}")

def extraer_datos(period="day"):
    conn = conectar_bd()
    start, end = get_date_range(period)
    s0, e0 = start.strftime("%Y-%m-%dT%H:%M:%S"), end.strftime("%Y-%m-%dT%H:%M:%S")

    # 1) TagUID → TechnicalName
    q_map = """
        WITH LV AS (
          SELECT TagUID, TechnicalName,
                 ROW_NUMBER() OVER(PARTITION BY TagUID ORDER BY Created DESC) rn
          FROM TLG.TagVersion
        )
        SELECT T.VTagUID AS TagUID,
               COALESCE(LV.TechnicalName,'Sin Nombre') AS TagName
        FROM TLG.Tag T
        LEFT JOIN LV ON LV.TagUID=T.TagUID AND LV.rn=1
    """
    df_map = pd.read_sql(q_map, conn)

    # 2) TagUID → (GroupName, Kind)
    q_rel = f"""
        DECLARE @P uniqueidentifier='{UID_PLANTA}', @B uniqueidentifier='{UID_CUENCA}';
        WITH R AS (
          SELECT RGT.TagUID,
                 GUPLT.Text   AS GroupName,
                 GT.GroupTypeUID
          FROM TLG.Rel_Group_Tag RGT
          INNER JOIN COMMON.GroupVersion GV ON GV.GroupUID=RGT.GroupUID
          INNER JOIN COMMON.GroupType GT      ON GT.GroupTypeUID=GV.GroupTypeUID
          OUTER APPLY COMMON.udf_GetUnpivotLanguageTexts(GV.NameResourceUID) GUPLT
          WHERE GUPLT.LanguageID=0
        )
        SELECT DISTINCT TagUID,
          GroupName,
          CASE WHEN GroupTypeUID=@P THEN 'plant'
               WHEN GroupTypeUID=@B THEN 'basin'
               ELSE 'other' END AS Kind
        FROM R
    """
    df_rel = pd.read_sql(q_rel, conn)

    # 3) Datos agregados según periodo
    if period=="day":
        # daily
        q = f"""
          SELECT
            CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS datetime2(0)) AS Date,
            TagUID,
            CASE WHEN Agg_NUM=0 THEN NULL
                 ELSE Agg_SUM/CAST(Agg_NUM AS float) END AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp BETWEEN '{s0}' AND '{e0}'
          ORDER BY Date
        """
        df = pd.read_sql(q, conn)
        # hourly
        qh = f"""
          SELECT
            CAST(DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0) AS datetime2(0)) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp BETWEEN '{s0}' AND '{e0}'
          GROUP BY DATEADD(hour, DATEDIFF(hour,0,SWITCHOFFSET(TimeStamp,'+00:00')),0), TagUID
          ORDER BY Date
        """
        df_hourly = pd.read_sql(qh, conn)

    else:
        # agrupar por día/mes/año según período
        if period in ("week","month"):
            grp = "CAST(SWITCHOFFSET(TimeStamp,'+00:00') AS date)"
        else:  # year
            grp = "DATEFROMPARTS(YEAR(SWITCHOFFSET(TimeStamp,'+00:00')), MONTH(SWITCHOFFSET(TimeStamp,'+00:00')),1)"
        q = f"""
          SELECT
            CAST({grp} AS datetime2(0)) AS Date,
            TagUID,
            AVG(CASE WHEN Agg_NUM=0 THEN NULL
                     ELSE Agg_SUM/CAST(Agg_NUM AS float) END) AS Value
          FROM TLG.VAggregateValue
          WHERE TimeStamp BETWEEN '{s0}' AND '{e0}'
          GROUP BY {grp}, TagUID
          ORDER BY Date
        """
        df = pd.read_sql(q, conn)
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    conn.close()

    # 4) Mapear nombres y grupos
    df       = df.merge(df_map, on='TagUID', how='left').merge(df_rel, on='TagUID', how='left')
    df_hourly= df_hourly.merge(df_map, on='TagUID', how='left').merge(df_rel, on='TagUID', how='left')

    return {'daily': df, 'hourly': df_hourly}

def guardar_json(resultados, filename='tags_data.json'):
    """
    resultados: dict(periodo -> {'daily':df, 'hourly':df})
    Genera data/tags_data.json con:
    {
      periodo: {
        daily:  [ {Timestamp, TagUID, Value, TagName, GroupName, Kind}, ... ],
        hourly: [...]
      }, ...
    }
    """
    path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','data',filename))
    os.makedirs(os.path.dirname(path), exist_ok=True)

    out = {}
    for per, recs in resultados.items():
        out[per] = {}
        for sub in ('daily','hourly'):
            df = recs.get(sub, pd.DataFrame())
            # renombrar Date→Timestamp
            if not df.empty:
                df2 = df.rename(columns={'Date':'Timestamp'})
                out[per][sub] = df2.to_dict(orient='records')
            else:
                out[per][sub] = []
    with open(path,'w',encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    logging.info("✅ JSON guardado en %s", path)
