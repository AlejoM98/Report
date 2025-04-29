# SCADA Reportes

Este proyecto se encarga de extraer datos desde una base de datos de WinCC (SQL Server), guardar la información en formato JSON, y generar reportes (diarios, semanales, mensuales, anuales) y una tabla en Excel.

## Estructura del Proyecto

- **config/**: Contiene el archivo `config.ini` con las credenciales de conexión.
- **data/**: Carpeta donde se guardan los archivos generados (JSON, Excel).
- **src/**: Código fuente del proyecto.
  - `conexion.py`: Conexión y extracción de datos desde SQL Server.
  - `scanner.py`: Escaneo periódico de datos.
  - `reportes.py`: Generación de reportes.
  - `tabla_dinamica.py`: Creación de tabla dinámica en Excel.
- **main.py**: Archivo principal para ejecutar el flujo completo.
- **tests/**: Pruebas unitarias.
- **logs/**: Registro de logs.
- **requirements.txt**: Dependencias del proyecto.
- **.gitignore**: Archivos y carpetas a ignorar en Git.

## Instrucciones

1. Configura tus credenciales en `config/config.ini`.
2. Instala las dependencias con:
   ```bash
   pip install -r requirements.txt
