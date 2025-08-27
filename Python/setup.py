from cx_Freeze import setup, Executable

setup(
    name="Registro88",
    version="1.0",
    description="App Registro 88",
    executables=[Executable("#REGISTRO - 88.py", base="Win32GUI")]
)
