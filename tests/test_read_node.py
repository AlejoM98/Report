from opcua import Client

# Aquí asumimos que el nodo se identifica como "EPSAS_EHAT" en el namespace 2.
node_id = "ns=2;s=EPSAS_EHAT/SERVIDOR02/Tags"
# Asegúrate de que esta URL sea la correcta (puede variar según la configuración de red).
server_url = "opc.tcp://192.168.9.14:4840"
# Crear el cliente OPC
client = Client(server_url)

try:
    # Conectar al servidor
    client.connect()
    print("✅ Conexión establecida al servidor OPC UA.")

    # Intentar obtener el nodo EPSAS_EHAT
    node = client.get_node(node_id)
    print(f"🔍 Nodo obtenido: {node}")

    # Leer el valor del nodo
    value = node.get_value()
    print(f"📊 Valor del nodo '{node_id}': {value}")

except Exception as e:
    print("❌ Error al leer el nodo:", e)
finally:
    try:
        client.disconnect()
        print("🔌 Desconectado del servidor OPC UA.")
    except Exception as e:
        print("❌ Error al desconectar:", e)
