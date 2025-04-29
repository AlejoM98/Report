import time
from datetime import datetime
from src.conexion import extraer_datos, guardar_json

def scanner(interval=300):
    while True:
        print(f"🔍 Escaneando datos... {datetime.now()}")
        datos = extraer_datos()
        guardar_json(datos)
        time.sleep(interval)

if __name__ == "__main__":
    scanner()
