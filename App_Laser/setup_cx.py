import sys
from cx_Freeze import setup, Executable
import os

# Archivos de datos e imagen a incluir
includefiles = [
    (os.path.join("..", "logo_dragon.png"), "logo_dragon.png"),
    (os.path.join("..", "DB_Partes.txt"), "DB_Partes.txt"),
    (os.path.join("..", "DB_Partes.txt.bak"), "DB_Partes.txt.bak"),
    (os.path.join("..", "DB_Registro_pendiente.txt"), "DB_Registro_pendiente.txt"),
    (os.path.join("..", "DB_Registro.txt"), "DB_Registro.txt"),
    (os.path.join("..", "DB_Registro.txt.bak"), "DB_Registro.txt.bak"),
    (os.path.join("..", "deregister parts.txt"), "deregister parts.txt"),
    (os.path.join("..", "deregister staff.txt"), "deregister staff.txt"),
    (os.path.join("..", "clave2.key"), "clave2.key")
]

# Dependencias adicionales
build_exe_options = {
    "packages": ["tkinter", "mysql", "PIL", "cryptography"],
    "include_files": includefiles,
    "excludes": ["unittest", "email", "html", "http", "xml", "pydoc_data", "test", "distutils"]
}


# Forzar sin terminal en Windows
base = "Win32GUI" if sys.platform == "win32" else None

setup(
    name = "RegistroLaser88",
    version = "1.0",
    description = "App de registro y bajas Laser88",
    options = {"build_exe": build_exe_options},
    executables = [Executable("#Registro88 - MYSQL.py", base=base, icon=None)]
)
