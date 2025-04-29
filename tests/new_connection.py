# test_driver.py
import pyodbc

try:
    print("Drivers instalados:")
    print(pyodbc.drivers())
except Exception as e:
    print(f"Error: {e}")