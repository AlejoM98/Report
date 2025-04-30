import json
import os

def validate_mapping(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: El archivo {file_path} no existe.")
        return

    # Listas para registrar violaciones
    grupos_en_ambas = []
    plantas_con_cuenca = []
    cuencas_con_planta = []

    # Verificar cada grupo en el JSON
    for group_key, data in mapping.items():
        has_plant = bool(data.get("Plant", "").strip()) 
        has_basin = bool(data.get("Basin", "").strip())  

        # Regla 1: Grupo en ambas categorías
        if has_plant and has_basin:
            grupos_en_ambas.append(group_key)
        
        # Regla 2 y 4: Plantas no pueden tener cuenca
        if has_plant and has_basin:
            plantas_con_cuenca.append(group_key)
        
        # Regla 3: Cuencas no pueden tener planta
        if has_basin and has_plant:
            cuencas_con_planta.append(group_key)

    # Mostrar resultados
    if grupos_en_ambas:
        print(f"❌ Regla 1 violada: Grupos en plantas Y cuencas → {grupos_en_ambas}")
    
    if plantas_con_cuenca:
        print(f"❌ Reglas 2/4 violadas: Plantas con cuenca → {plantas_con_cuenca}")
    
    if cuencas_con_planta:
        print(f"❌ Regla 3 violada: Cuencas con planta → {cuencas_con_planta}")

    # Éxito si no hay violaciones
    if not grupos_en_ambas and not plantas_con_cuenca and not cuencas_con_planta:
        print("✅ Todas las reglas se cumplen correctamente.")

# --------------------------------------------------------------------------
# Ejecutar validación
# --------------------------------------------------------------------------
file_path = os.path.join("config", "tag_mapping_new.json")
validate_mapping(file_path)