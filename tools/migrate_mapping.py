import csv
import json
import os

# Configurar rutas
input_csv = os.path.join("config", "group_mapping.csv")
output_json = os.path.join("config", "tag_mapping_new.json")

# Diccionario para clasificar grupos como plantas o cuencas
GROUP_TYPE = {
    "Achachicala": "plant",
    "El_Alto": "plant",
    "Tilata": "plant",
    "Pampahasi": "plant",
    "Chuquiaguillo": "plant",
    "LineaOeste": "plant",
    "SanFelipe": "plant",
    "25Junio": "plant",
    "NevadaFatic": "plant",
    "Lirios": "plant",
    "Tuni": "basin",
    "Milluni": "basin",
    "Hampaturi": "basin",
    "Palcoma": "basin",
    "Kaluyo": "basin",
    "Incachaca": "basin",
    # ... Completa con todos los grupos
}

# Procesar CSV
plants = {}
basins = {}

with open(input_csv, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        group_name = row['GroupName']
        taguid = row['TagUID'].lower().replace('-', '_')  # Formato: 511e75f9_6b8e_4807_bca1_0c97fb5c5aa0
        
        if group_name in GROUP_TYPE:
            if GROUP_TYPE[group_name] == "plant":
                plants[taguid] = group_name
            else:
                basins[taguid] = group_name

# Crear JSON final
mapping = {
    "plants": plants,
    "basins": basins
}

with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(mapping, f, indent=2, ensure_ascii=False)

print(f"✅ Mapeo generado en: {output_json}")