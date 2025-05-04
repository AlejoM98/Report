import configparser
import pyodbc
import pandas as pd
import json
import os
import time
import logging
from datetime import datetime, timedelta, timezone

# ── Configuración de logging ────────────────────────────────────────────────────
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'app.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# ── GUIDs de tus tipos de grupo: planta vs cuenca ──────────────────────────────
# Sustituye estos valores con los reales de tu base de datos
UID_PLANTA = '19CDF42E-1E2E-4DBF-0001-000000000001'
UID_CUENCA = '19CDF42E-1E2E-4DBF-0001-000000000002'

def leer_config():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
    return cfg

def conectar_bd(retries: int = 5, backoff_base: float = 2.0):
    cfg      = leer_config()
    server   = cfg['DATABASE']['server']
    database = cfg['DATABASE']['database']
    driver   = cfg['DATABASE']['driver']
    auth     = cfg['DATABASE'].get('auth_mode', 'windows').lower()

    if auth == 'windows':
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        user = cfg['DATABASE']['username']
        pwd  = cfg['DATABASE']['password']
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={pwd};"

    for attempt in range(1, retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            logging.info("✅ Conexión exitosa en intento %d", attempt)
            return conn
        except pyodbc.Error as e:
            logging.error("❌ Error ODBC intento %d: %s", attempt, e)
        time.sleep(backoff_base ** (attempt - 1))

    logging.critical("No se pudo conectar tras %d intentos", retries)
    return None

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

def extraer_datos(period='day'):
    conn = conectar_bd()
    if not conn:
        raise ConnectionError("No se estableció conexión con la base de datos.")

    # Rangos de fecha para la consulta
    start, end = get_date_range(period)
    s0, e0 = start.strftime("%Y-%m-%dT%H:%M:%S"), end.strftime("%Y-%m-%dT%H:%M:%S")

    # ── 1) Mapeo TagUID → TagName ───────────────────────────────────────────────
    q_map = """
      WITH LatestVersions AS (
        SELECT TV.TagUID, TV.TechnicalName,
               ROW_NUMBER() OVER (PARTITION BY TV.TagUID ORDER BY TV.Created DESC) AS rn
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

    # ── 2) Mapeo real TagUID → Planta / Cuenca ────────────────────────────────
    q_groups = f"""
    DECLARE @UID_PLANTA UNIQUEIDENTIFIER = '{UID_PLANTA}';
    DECLARE @UID_CUENCA  UNIQUEIDENTIFIER = '{UID_CUENCA}';

    WITH Rel AS (
      SELECT DISTINCT
        RGT.TagUID,
        GUPLT.Text        AS GroupName,
        GT.GroupTypeUID
      FROM TLG.Rel_Group_Tag AS RGT
      INNER JOIN COMMON.[Group]      AS G   ON RGT.GroupUID   = G.GroupUID
      INNER JOIN COMMON.GroupVersion AS GV  ON GV.GroupUID     = G.GroupUID
      INNER JOIN COMMON.GroupType    AS GT  ON GT.GroupTypeUID = GV.GroupTypeUID
      OUTER APPLY COMMON.udf_GetUnpivotLanguageTexts(GV.NameResourceUID) AS GUPLT
      WHERE GUPLT.LanguageID = 0
        AND GT.GroupTypeUID IN (@UID_PLANTA, @UID_CUENCA)
    )
    SELECT
      TagUID,
      GroupName,
      CASE
        WHEN GroupTypeUID = @UID_PLANTA THEN 'plant'
        WHEN GroupTypeUID = @UID_CUENCA  THEN 'basin'
      END AS Kind
    FROM Rel;
    """
    df_groups = pd.read_sql(q_groups, conn)

    tag_to_plant = {
        row.TagUID: row.GroupName
        for _, row in df_groups[df_groups.Kind == 'plant'].iterrows()
    }
    tag_to_basin = {
        row.TagUID: row.GroupName
        for _, row in df_groups[df_groups.Kind == 'basin'].iterrows()
    }

    # ── 3) Consultas de datos según periodo ────────────────────────────────────
    if period == 'day':
        q_daily = f"""
            SELECT
              SWITCHOFFSET(AV.TimeStamp,'+00:00') AS Date,
              AV.TagUID,
              CASE WHEN AV.Agg_NUM=0 THEN NULL
                   ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
              END AS Value
            FROM TLG.VAggregateValue AV
            WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
            ORDER BY AV.TimeStamp;
        """
        df_daily  = pd.read_sql(q_daily, conn)

        q_hourly = f"""
            SELECT
              DATEADD(hour, DATEDIFF(hour,0, SWITCHOFFSET(AV.TimeStamp,'+00:00')),0) AS Date,
              AV.TagUID,
              AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL
                       ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
            FROM TLG.VAggregateValue AV
            WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
            GROUP BY DATEADD(hour, DATEDIFF(hour,0, SWITCHOFFSET(AV.TimeStamp,'+00:00')),0), AV.TagUID;
        """
        df_hourly = pd.read_sql(q_hourly, conn)

    elif period in ('week','month'):
        q_daily = f"""
            SELECT
              CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date) AS Date,
              AV.TagUID,
              AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL
                       ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
            FROM TLG.VAggregateValue AV
            WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
            GROUP BY CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date), AV.TagUID
            ORDER BY Date;
        """
        df_daily  = pd.read_sql(q_daily, conn)
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    elif period == 'year':
        q_daily = f"""
            SELECT
              DATEFROMPARTS(YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')),
                            MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')),1) AS Date,
              AV.TagUID,
              AVG(CASE WHEN AV.Agg_NUM=0 THEN NULL
                       ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float) END) AS Value
            FROM TLG.VAggregateValue AV
            WHERE AV.TimeStamp >= '{s0}' AND AV.TimeStamp < '{e0}'
            GROUP BY DATEFROMPARTS(YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')),
                                   MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')),1), AV.TagUID
            ORDER BY Date;
        """
        df_daily  = pd.read_sql(q_daily, conn)
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value'])

    else:
        conn.close()
        raise ValueError(f"Periodo no soportado: {period}")

    # ── 4) Asignar metadatos y mapeo ────────────────────────────────────────────
    for df in (df_daily, df_hourly):
        if df.empty:
            continue
        df['TagName'] = df['TagUID'].map(name_map).fillna('Sin Nombre')
        df['Plant']   = df['TagUID'].map(tag_to_plant).fillna('')
        df['Basin']   = df['TagUID'].map(tag_to_basin).fillna('')

    conn.close()

    return {
        'daily':  {'raw': df_daily,  'pivot': df_daily.pivot_table(index='Date', columns='TagName', values='Value')},
        'hourly': {'raw': df_hourly, 'pivot': df_hourly.pivot_table(index='Date', columns='TagName', values='Value')}
    }

def guardar_json(resultados, filename='tags_data.json'):
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', filename))
    os.makedirs(os.path.dirname(path), exist_ok=True)

    out = {}
    for period, data in resultados.items():
        out_period = {}
        for sub in ['daily','hourly']:
            df = data.get(sub, {}).get('raw', pd.DataFrame())
            out_period[sub] = [
                {
                  'TagName':   r.TagName,
                  'Value':     r.Value,
                  'Timestamp': r.Date.isoformat() if hasattr(r.Date, 'isoformat') else str(r.Date),
                  'Plant':     r.Plant,
                  'Basin':     r.Basin
                }
                for _, r in df.iterrows()
            ]
        out[period] = out_period

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)

    print(f"✅ JSON guardado en {path}")
