# src/conexion.py

import configparser, pyodbc, pandas as pd, json, os, time, logging
from datetime import datetime, timedelta, timezone
import warnings

# Suprimir warnings de pandas sobre DBAPI
warnings.filterwarnings("ignore", category=UserWarning, module='pandas.io.sql')

logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), '..', 'app.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Para exponer tu mapeo TagUID→TagName si lo necesitas en main.py
df_map: pd.DataFrame = None

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
    cfg.read(os.path.join(os.path.dirname(__file__),'..','config','config.ini'))
    return cfg

def conectar_bd(retries: int = 5, backoff_base: float = 2.0):
    cfg      = leer_config()
    driver   = cfg['DATABASE']['driver']
    server   = cfg['DATABASE']['server']
    database = cfg['DATABASE']['database']
    auth     = cfg['DATABASE'].get('auth_mode','windows').lower()
    if auth=='windows':
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        u = cfg['DATABASE']['username']
        p = cfg['DATABASE']['password']
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={u};PWD={p};"

    for i in range(1, retries+1):
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            logging.info("✅ Conexión exitosa (intento %d)", i)
            return conn
        except pyodbc.Error as e:
            logging.error("❌ Error ODBC intento %d: %s", i, e)
        time.sleep(backoff_base ** (i-1))

    logging.critical("No pudo conectar tras %d intentos", retries)
    return None

def extraer_datos(period='day'):
    global df_map
    conn = conectar_bd()
    if not conn:
        raise ConnectionError("No se estableció conexión con la base de datos.")

    start, end = get_date_range(period)
    s0, e0     = start.strftime("%Y-%m-%dT%H:%M:%S"), end.strftime("%Y-%m-%dT%H:%M:%S")

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
    df_map_local = pd.read_sql(q_map, conn)
    df_map       = df_map_local
    name_map     = dict(zip(df_map_local.TagUID, df_map_local.TagName))

    # 2) Cargar mapeo planta/cuenca
    mapping_path = os.path.join(os.path.dirname(__file__),'..','config','tag_mapping_new.json')
    maps         = json.load(open(mapping_path, encoding='utf-8'))
    plants_map   = {k.lower():v for k,v in maps['plants'].items()}
    basins_map   = {k.lower():v for k,v in maps['basins'].items()}

    # 3) Consultas según periodo
    if period == 'day':
        # ▶ Diarios
        qd = f"""
          SELECT
            CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS datetime2) AS Date,
            AV.TagUID,
            CASE WHEN AV.Agg_NUM=0 THEN NULL
                 ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
            END AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp BETWEEN '{s0}' AND '{e0}'
          ORDER BY AV.TimeStamp;
        """
        df_daily = pd.read_sql(qd, conn)

        # ▶ Horarios (GROUP BY expresiones completas + ORDER BY dentro)
        qh = f"""
          SELECT
            DATEADD(
              hour,
              DATEDIFF(hour, 0, SWITCHOFFSET(AV.TimeStamp,'+00:00')),
              0
            ) AS Date,
            AV.TagUID,
            AVG(
              CASE WHEN AV.Agg_NUM = 0 THEN NULL
                   ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
              END
            ) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp BETWEEN '{s0}' AND '{e0}'
          GROUP BY
            DATEADD(hour, DATEDIFF(hour, 0, SWITCHOFFSET(AV.TimeStamp,'+00:00')), 0),
            AV.TagUID
          ORDER BY
            DATEADD(hour, DATEDIFF(hour, 0, SWITCHOFFSET(AV.TimeStamp,'+00:00')), 0);
        """
        df_hourly = pd.read_sql(qh, conn)

    elif period in ('week','month'):
        qd = f"""
          SELECT
            CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date) AS Date,
            AV.TagUID,
            AVG(
              CASE WHEN AV.Agg_NUM=0 THEN NULL
                   ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
              END
            ) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp BETWEEN '{s0}' AND '{e0}'
          GROUP BY CAST(SWITCHOFFSET(AV.TimeStamp,'+00:00') AS date), AV.TagUID
          ORDER BY Date;
        """
        df_daily  = pd.read_sql(qd, conn)
        # <-- Aquí agregamos TagName, Plant, Basin aunque esté vacío
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value','TagName','Plant','Basin'])

    else:  # period == 'year'
        qd = f"""
          SELECT
            DATEFROMPARTS(
              YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')),
              MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')),
              1
            ) AS Date,
            AV.TagUID,
            AVG(
              CASE WHEN AV.Agg_NUM=0 THEN NULL
                   ELSE AV.Agg_SUM/CAST(AV.Agg_NUM AS float)
              END
            ) AS Value
          FROM TLG.VAggregateValue AV
          WHERE AV.TimeStamp BETWEEN '{s0}' AND '{e0}'
          GROUP BY
            DATEFROMPARTS(YEAR(SWITCHOFFSET(AV.TimeStamp,'+00:00')),
                          MONTH(SWITCHOFFSET(AV.TimeStamp,'+00:00')), 1),
            AV.TagUID
          ORDER BY Date;
        """
        df_daily  = pd.read_sql(qd, conn)
        # <-- Añadimos mismas columnas para no romper pivot
        df_hourly = pd.DataFrame(columns=['Date','TagUID','Value','TagName','Plant','Basin'])

    # 4) Mapeo TagName → Plant/Basin y advertencias
    for df in (df_daily, df_hourly):
        if df.empty:
            continue
        df['TagName'] = df.TagUID.map(name_map).fillna('Sin Nombre')
        df['NUID']    = df.TagUID.str.lower().str.replace('-', '_')
        df['Plant']   = df.NUID.map(plants_map).fillna('')
        df['Basin']   = df.NUID.map(basins_map).fillna('')
        unmapped = set(df.NUID.unique()) - set(plants_map) - set(basins_map)
        if unmapped:
            logging.warning("NUIDs sin mapeo (Plant/Basin): %s", sorted(unmapped)[:20])
        df.drop(columns=['NUID'], inplace=True)

    conn.close()

    # 5) Pivot seguro
    daily_pivot  = df_daily.pivot_table(index='Date', columns='TagName', values='Value')
    hourly_pivot = df_hourly.pivot_table(index='Date', columns='TagName', values='Value')

    return {
        'daily':  {'raw': df_daily,  'pivot': daily_pivot},
        'hourly': {'raw': df_hourly, 'pivot': hourly_pivot}
    }

def guardar_json(resultados, filename='tags_data.json'):
    path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','data',filename))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    out = {}
    for per, data in resultados.items():
        entry = {}
        for sub in ['daily','hourly']:
            df = data[sub]['raw']
            entry[sub] = [
                {
                  'TagName':   r.TagName,
                  'Value':     r.Value,
                  'Timestamp': r.Date.isoformat(),
                  'Plant':     r.Plant,
                  'Basin':     r.Basin
                }
                for _, r in df.iterrows()
            ]
        out[per] = entry
    with open(path,'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("✅ JSON guardado en", path)
