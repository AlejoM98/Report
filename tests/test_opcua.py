from opcua import Client

server_url = "opc.tcp://192.168.9.14:4840"
client = Client(server_url)

try:
    client.connect()
    print("✅ Conectado exitosamente al servidor OPC UA")
    
    root = client.get_root_node()
    print(f"🔍 Nodo raíz: {root}")

except Exception as e:
    print(f"❌ Error al conectar: {e}")
finally:
    try:
        client.disconnect_socket()
        print("🔌 Desconectado del servidor OPC UA (socket cerrado)")
    except Exception as e:
        print(f"❌ Error al cerrar socket: {e}")
