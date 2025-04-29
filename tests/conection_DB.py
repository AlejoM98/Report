from sqlalchemy import create_engine
import pandas as pd

# Configura la cadena de conexión
engine = create_engine("mssql+pyodbc://sa:Epsas12345$@192.168.9.14\\HISTORIAN/HistorianStorage?driver=ODBC+Driver+17+for+SQL+Server")

try:
    with engine.connect() as conn:
        print("Conexión exitosa a la base de datos.")

        # Ejemplo de consulta
        query = "SELECT TOP 10 * ,ServerTimestamp AT TIME ZONE 'UTC' AS columna_datetime FROM [TLG].[TagVersion]"
        df = pd.read_sql(query, conn)
        print(df.head())

except Exception as e:
    print("Error al conectar o consultar:", e)
