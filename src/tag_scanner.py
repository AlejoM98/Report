import json
import os

class TagScanner:
    def __init__(self, config_file=None):
        if config_file is None:
            config_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'tags_config.json')
        self.config_file = config_file
        self.tags_config = self._load_tags_config()

    def _load_tags_config(self):
        if not os.path.exists(self.config_file):
            print(f"El archivo de configuración de tags {self.config_file} no existe.")
            return []
        with open(self.config_file, "r") as f:
            return json.load(f)

    def get_specific_tags(self, tag_names):
        return [tag for tag in self.tags_config if tag.get("tag_name") in tag_names]

    def list_all_tags(self):
        return self.tags_config

if __name__ == "__main__":
    scanner = TagScanner()
    print("Todos los tags configurados:")
    print(scanner.list_all_tags())
    specific = scanner.get_specific_tags(["Temp_Sensor_1", "Pressure_Valve"])
    print("Tags específicos:", specific)
