from opcua import Client
import pandas as pd
from datetime import datetime
import configparser

class OPCUAClient:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config/config.ini')
        self.url = config['OPCUA']['server_url']
        self.client = Client(self.url)
        self.namespace = config['OPCUA']['namespace']

    def connect(self):
        try:
            self.client.connect()
            print("✅ Conexión OPC UA establecida")
        except Exception as e:
            print(f"❌ Error OPC UA: {e}")

    def obtener_datos(self):
        # Implementa aquí la lógica específica para tus tags OPC UA
        # Ejemplo:
        node = self.client.get_node(f"ns={self.namespace};s=TuTagOPC")
        return pd.DataFrame({
            'Valor': [node.get_value()],
            'Timestamp': [datetime.now()]
        })

    def disconnect(self):
        self.client.disconnect()