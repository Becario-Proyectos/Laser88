#LASER - 88
import tkinter as tk
from tkinter import messagebox, filedialog, scrolledtext
import os
from datetime import datetime
import sys
import time
import snap7
import threading
import tkinter.ttk as ttk
from tkinter import PhotoImage
import os
import csv
from cryptography.fernet import Fernet
import mysql.connector

# ============================= RUTAS / ARCHIVOS =============================
NOMBRE_ARCHIVO_REGISTROS = "C:/VCST/2888/Registros/RegistroPersonal/DB_Registro.txt" 
NOMBRE_ARCHIVO_PARTES   = "C:/VCST/2888/Registros/RegistroPartes/DB_Partes.txt"

NOMBRE_ARCHIVO_REGISTROS_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Registro_pendiente.txt"
NOMBRE_ARCHIVO_BAJAS_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Bajas_pendiente.txt"

NOMBRE_ARCHIVO_LOGS     = "C:/VCST/2888/Logs/RegistroLogs/Register_Logs.csv"
NOMBRE_ARCHIVO_LOGS_ERRORES = "C:/VCST/2888/Logs/ErrorLogs/Error_Logs.csv"

LASER_CODE_FILE_PATH    = "C:/VCST/2888/Laser/active_laser_code.csv"
PRODUCT_CSV_BASE_PATH   = "C:/VCST/Aplicaciones/2888/Data"

SERIAL_NUMBER_LOG_PATH  = "C:/VCST/2888/Laser/Serial_Numbers.csv" 

#Serial Number sin Cifrar
UNENCRYPTED_SERIAL_NUMBER_PATH = "C:/VCST/2888/Laser/Serial_Numbers_Unencrypted.csv"

# CAT Number paths
CAT_NUMBER_LOG_PATH = "C:/VCST/2888/Laser/CAT_Number.csv"
UNENCRYPTED_CAT_NUMBER_PATH = "C:/VCST/2888/Laser/CAT_Number_Unencrypted.csv"
CLAVE_PATH              = "C:/VCST/2888/Data/Key/clave2.key"       

# ============================= PLC CONFIG ==================================
PLC_IP = '192.168.50.20'  #La de la laser es 192.168.21.20
RACK = 0
SLOT = 1
DB_NUMBER = 17
READ_INTERVAL_SECONDS = 2

# Heartbeat en DBX1.0 (DBB1 completo)
HEARTBEAT_DB = DB_NUMBER
HEARTBEAT_BYTE_OFFSET = 1    
HB_PULSE_SECONDS = 0.10      

# ============================== COLORES ==============================
COLOR_BACKGROUND_PRIMARY   = "#2C3E50"
COLOR_BACKGROUND_SECONDARY = "#34495E"
COLOR_BACKGROUND_TERTIARY  = "#4A627A"

COLOR_TEXT_PRIMARY = "#ECF0F1"
COLOR_TEXT_LIGHT   = "#ECF0F1"
COLOR_TEXT_MUTED   = "#BDC7BD"
COLOR_ACCENT_BLUE  = "#FFFFFF"
COLOR_SUCCESS_GREEN= "#00FF22"
COLOR_DANGER_RED   = "#FF3131"
COLOR_BORDER_SUBTLE= "#5A738D"
COLOR_FOCUS_BORDER = "#3498DB"

COLOR_PLC_CONNECTED    = "#00FF22"
COLOR_PLC_DISCONNECTED = "#ED1919"
COLOR_PLC_WARNING      = "#F1C40F"

COLOR_SUBMENU_BACKGROUND = "#D3D3D3"
COLOR_SUBMENU_FOREGROUND = "#000000"

# ============================= FUENTES =================================
FONT_TITLE_APP       = ("Segoe UI", 28, "bold")
FONT_SECTION_HEADER  = ("Segoe UI", 14, "bold")
FONT_LABEL           = ("Segoe UI", 13, "bold")
FONT_LABEL_BOLD      = ("Segoe UI", 13, "bold")
FONT_ENTRY           = ("Segoe UI", 13)
FONT_BUTTON          = ("Segoe UI", 13, "bold")
FONT_STATUS_INFO     = ("Consolas", 18, "bold")
FONT_STATUS_ERROR    = ("Consolas", 18, "bold")

# ============================= ICONO PERSONALIZADO ==============================
ICONO_PATH = r"C:/VCST/Aplicaciones/2888/Icon/icono_dragon.ico"  # Ruta definitiva

def set_custom_icon(window):
    try:
        if os.path.exists(ICONO_PATH):
            window.iconbitmap(ICONO_PATH)
    except Exception as e:
        write_log("ERROR", f"Error al establecer icono personalizado: {e}")
        pass  # Si falla, simplemente ignora y usa el icono por defecto

root_machine_app = None
login_successful_machine = False
plc_client = None
monitoring_thread = None
stop_monitoring_event = threading.Event()

last_read_product_counter = None
last_product_time = None
last_plc_connection_status = False
plc_disconnection_time = None

plc_status_text_var = None
plc_connection_indicator_canvas = None
plc_connection_indicator_oval_id = None
plc_last_update_label_var = None
plc_error_label_widget = None
plc_status_label_widget = None

product_id_var = None
product_counter_var = None
product_nok_counter_var = None
product_read_time_display_var = None
product_height_var = None

measurement2_var = None
measurement3_var = None
measurement4_var = None
additional_info1_var = None
additional_info2_var = None
serial_number_var = None

logged_in_user_name_global = None
logged_in_laser_code_global = None
logged_in_datetime_global = None
logged_in_user_number_global = None 

download_menu_instance = None
logs_menu_instance = None
serial_menu_instance = None

# ============================= CONTADOR DIARIO DE PIEZAS =============================
daily_piece_counter = 0
current_day_for_counter = None

# ============================= MySQL CONFIG / TABLAS =============================
DB_HOST = "10.4.0.103"
DB_USER = "wamp_user"
DB_PASSWORD = "wamp"
DB_NAME = "test"
TABLE_USERS = "Registered_personnel"
TABLE_TRACEABILITY_CATERPILLAR = "Traceability_Caterpillar"  

# ========================== LOG_STATUS EN PLC =============================
def set_log_status_in_plc(value: bool):
    global plc_client
    try:
        if not plc_client or not plc_client.get_connected():
            if not connect_to_plc():
                return False
        # Leer el primer byte (offset 0)
        data = bytearray(plc_client.db_read(DB_NUMBER, 0, 1))       #ALTERNO A GET_BOOL CON SNAP7
        if value:
            data[0] |= 0x08  #EQUIVALENTE A "Log_Status = 1"
        else:
            data[0] &= 0xF7  #EQUIVALENTE A "Log_Status = 0"
        plc_client.db_write(DB_NUMBER, 0, data)
        return True
    except Exception as e:
        write_log("ERROR", f"Error al escribir Log_Status en PLC: {e}")
        return False

# =============================== UTILIDADES LOG =============================
def write_log(event_type, message):
    import csv
    import os
    try:
        now = datetime.now()
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        year_month = now.strftime("%Y-%m")

        # Carpeta y archivo para logs de registros
        logs_dir = os.path.join(os.path.dirname(NOMBRE_ARCHIVO_LOGS), year_month)
        os.makedirs(logs_dir, exist_ok=True)
        logs_file = os.path.join(logs_dir, os.path.basename(NOMBRE_ARCHIVO_LOGS))
        with open(logs_file, "a", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([ts, event_type, message])

        # Carpeta y archivo para logs de errores
        if event_type in ("ERROR", "WARNING", "AUTH_BLOCKED") or event_type.startswith("ERROR") or event_type.startswith("WARNING"):
            error_dir = os.path.join(os.path.dirname(NOMBRE_ARCHIVO_LOGS_ERRORES), year_month)
            os.makedirs(error_dir, exist_ok=True)
            error_file = os.path.join(error_dir, os.path.basename(NOMBRE_ARCHIVO_LOGS_ERRORES))
            with open(error_file, "a", encoding="utf-8", newline='') as f2:
                writer2 = csv.writer(f2)
                writer2.writerow([ts, event_type, message])
    except Exception as e:
        try:
            messagebox.showerror("Error de Logs",
                                 f"No se pudo escribir en logs:\n{NOMBRE_ARCHIVO_LOGS}\n\n{e}",
                                 parent=root_machine_app)
        except:
            pass

def read_logs():
    import csv
    import os
    from datetime import datetime
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    logs_dir = os.path.join(os.path.dirname(NOMBRE_ARCHIVO_LOGS), year_month)
    logs_file = os.path.join(logs_dir, os.path.basename(NOMBRE_ARCHIVO_LOGS))
    if not os.path.exists(logs_file):
        return ["No hay registros previos."]
    try:
        with open(logs_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            # Solo mostrar eventos de sesión y app
            return [", ".join(row) for row in reader if len(row) > 1 and row[1] in ("LOGIN", "LOGOUT", "APP_EXIT")]
    except Exception as e:
        write_log("ERROR", f"Error al leer logs: {e}")
        return [f"Error al leer logs: {e}"]

# Nueva función para leer logs de errores
def read_error_logs():
    import csv
    import os
    from datetime import datetime
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    error_dir = os.path.join(os.path.dirname(NOMBRE_ARCHIVO_LOGS_ERRORES), year_month)
    error_file = os.path.join(error_dir, os.path.basename(NOMBRE_ARCHIVO_LOGS_ERRORES))
    if not os.path.exists(error_file):
        return ["No hay logs de errores previos."]
    try:
        with open(error_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            return [", ".join(row) for row in reader]
    except Exception as e:
        write_log("ERROR", f"Error al leer logs de errores: {e}")
        return [f"Error al leer logs de errores: {e}"]

# ============================== CONEXIÓN MySQL ==============================
def _mysql_get_conn():
    try:
        return mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
    except Exception as e:
        write_log("ERROR", f"Error de conexión MySQL: {e}")
        return None

def _mysql_init_schema():
    try:
        root = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
        cur = root.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cur.close(); root.close()
    except Exception as e:
        write_log("ERROR", f"Init schema error: {e}")

def _mysql_init_traceability_table():
    try:
        conn = _mysql_get_conn()
        if not conn:
            write_log("WARNING", "No se pudo conectar a MySQL para crear tabla de trazabilidad")
            return False
        
        cur = conn.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_TRACEABILITY_CATERPILLAR} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fecha DATE NOT NULL,
            hora TIME NOT NULL,
            maquina VARCHAR(10) NOT NULL,
            operador VARCHAR(50),
            numero_parte VARCHAR(100),
            numero_serial VARCHAR(50),
            altura_mm DECIMAL(10,2),
            cat_number VARCHAR(100),
            tiempo_entre_productos_seg INT DEFAULT NULL,
            piezas_marcadas_dia INT DEFAULT NULL,
            timestamp_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fecha (fecha),
            INDEX idx_operador (operador),
            INDEX idx_numero_parte (numero_parte),
            INDEX idx_numero_serial (numero_serial)
        )
        """
        cur.execute(create_table_sql)
        cur.close(); conn.close()
        return True
    except Exception as e:
        write_log("ERROR", f"Error creando tabla de producción: {e}")
        try:
            cur.close(); conn.close()
        except Exception as e2:
            write_log("ERROR", f"Error cerrando conexión MySQL tras fallo en crear tabla: {e2}")
        return False

def insert_traceability_data_to_mysql(fecha, hora, operador, numero_parte, numero_serial, altura_mm, cat_number, tiempo_entre_productos, piezas_dia):
    try:
        conn = _mysql_get_conn()
        if not conn:
            write_log("WARNING", "No se pudo conectar a MySQL para insertar datos de trazabilidad")
            return False

        cur = conn.cursor()
        insert_sql = f"""
        INSERT INTO {TABLE_TRACEABILITY_CATERPILLAR} 
        (fecha, hora, maquina, operador, numero_parte, numero_serial, altura_mm, cat_number, tiempo_entre_productos_seg, piezas_marcadas_dia)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_sql, (fecha, hora, "2888", operador, numero_parte, numero_serial, altura_mm, cat_number, tiempo_entre_productos, piezas_dia))
        conn.commit()
        cur.close(); conn.close()
        write_log("INFO", f"Datos de trazabilidad insertados en MySQL - Parte: {numero_parte}, Serial: {numero_serial}")
        return True
    except Exception as e:
        write_log("ERROR", f"Error insertando datos de trazabilidad en MySQL: {e}")
        try:
            cur.close(); conn.close()
        except Exception as e2:
            write_log("ERROR", f"Error cerrando conexión MySQL tras fallo en insert: {e2}")
        return False

def save_individual_product_csv(fecha, hora, operador, numero_parte, numero_serial, altura_mm, cat_number=""):
    """
    Guarda un archivo CSV individual por cada marcado en la nueva ruta:
    C:/VCST/Aplicaciones/2888/Data/{YYYY-MM}/DD-MM-YYYY_HH-MM_{NumParte}.csv
    
    Contenido: Fecha, Hora, Maquina, Operador, Nparte, Nserial, Altura, CAT_number
    """
    try:
        # Crear timestamp para el nombre del archivo
        current_datetime = datetime.now()
        year_month = current_datetime.strftime("%Y-%m")
        
        # Formato del nombre: DD-MM-YYYY_HH-MM_NumParte.csv
        filename_timestamp = current_datetime.strftime("%d-%m-%Y_%H-%M")
        filename = f"{filename_timestamp}_{numero_parte}.csv"
        
        # Carpeta por mes dentro de la nueva ruta base
        month_dir = os.path.join(PRODUCT_CSV_BASE_PATH, year_month)
        os.makedirs(month_dir, exist_ok=True)
        
        # Ruta completa del archivo
        file_path = os.path.join(month_dir, filename)
        
        # Escribir CSV sin cifrar
        with open(file_path, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            # Headers
            writer.writerow(["Fecha", "Hora", "Maquina", "Operador", "Nparte", "Nserial", "Altura", "CAT_number"])
            # Data
            writer.writerow([fecha, hora, "2888", operador, numero_parte, numero_serial, f"{altura_mm:.2f}", cat_number])
        
        write_log("INFO", f"Archivo CSV individual creado: {filename}")
        return True, f"CSV guardado: {filename}"
        
    except Exception as e:
        write_log("ERROR", f"Error al crear archivo CSV individual: {e}")
        return False, f"Error: {e}"

def get_time_between_products():
    """
    Calcula el tiempo en segundos desde el último producto marcado.
    Retorna None si es el primer producto del día/sesión.
    """
    global last_product_time
    if last_product_time is None:
        return None
    
    current_time = datetime.now()
    time_diff = current_time - last_product_time
    return int(time_diff.total_seconds())

def update_daily_piece_counter():
    """
    Actualiza el contador diario de piezas. Se reinicia automáticamente cada día.
    Retorna el número actual de piezas marcadas en el día.
    """
    global daily_piece_counter, current_day_for_counter
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Verificar si cambió el día (reiniciar contador)
    if current_day_for_counter != current_date:
        current_day_for_counter = current_date
        daily_piece_counter = 0
        write_log("INFO", f"Contador diario reiniciado para el día: {current_date}")
    
    # Incrementar contador
    daily_piece_counter += 1
    
    return daily_piece_counter

def check_credentials_from_mysql(numero_registro_input, password_input):
    conn = _mysql_get_conn()
    if not conn:
        return None, None
    try:
        cur = conn.cursor()
        # Ajusta los nombres de columnas si difieren en tu tabla
        cur.execute(
            "SELECT Nombre, Code_Laser FROM registered_personnel WHERE Numero=%s AND Password=%s",
            (int(numero_registro_input), password_input)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return True, (row[0], row[1])
        return False, None
    except Exception as e:
        write_log("ERROR", f"Login MySQL error: {e}")
        try:
            cur.close(); conn.close()
        except Exception as e2:
            write_log("ERROR", f"Error cerrando conexión MySQL tras fallo en login: {e2}")
        return None, None

# =========================== TXT CIFRADO (Fernet) ===========================
def load_key():
    try:
        with open(CLAVE_PATH, "rb") as f:
            return f.read()
    except Exception as e:
        messagebox.showerror("Clave de cifrado",
                             f"No se pudo cargar la clave:\n{CLAVE_PATH}\n\n{e}",
                             parent=root_machine_app)
        return None

def leer_registros_descifrados(archivo):
    key = load_key()
    if not key:
        return []
    try:
        fernet = Fernet(key)
    except Exception as e:
        messagebox.showerror("Clave de cifrado",
                             f"Clave inválida/corrupta:\n{e}",
                             parent=root_machine_app)
        return []
    if not os.path.exists(archivo):
        # Para archivos de pendientes, esto es normal si no hay registros pendientes
        if "pendiente" in archivo.lower():
            return []
        # Para el archivo principal, mostrar advertencia
        write_log("WARNING", f"Archivo de registros no encontrado: {archivo}")
        return []

    registros = []
    try:
        with open(archivo, "rb") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    dec = fernet.decrypt(raw).decode("utf-8")
                    registros.append(dec)
                except Exception as e:
                    write_log("ERROR", f"Error al descifrar línea de {archivo}: {e}")
                    continue
    except Exception as e:
        write_log("ERROR", f"Error leyendo archivo {archivo}: {e}")
        return []
    return registros

def check_user_in_pending_deletions(numero_registro_input):
    bajas_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
    for line in bajas_pendientes:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')

        # Buscar el índice del número (entero) porque el nombre puede tener comas
        num_index = -1
        for i in range(2, len(parts)):
            if parts[i].strip().isdigit():
                num_index = i
                break
        if num_index == -1:
            continue

        numero = parts[num_index].strip()
        if numero == numero_registro_input:
            return True  # Usuario encontrado en bajas pendientes - NO debe poder acceder
    
    return False  # Usuario NO está en bajas pendientes - puede acceder si las credenciales son correctas

def check_credentials_from_txt_encrypted(numero_registro_input, password_input):
    """
    Formato esperado tras descifrar:
      fecha,Nombre(…puede tener comas…),Numero,Password,Code_Laser
    
    Busca primero en el archivo principal, luego en pendientes para soporte offline completo.
    """
    # Buscar en archivo principal
    registros_principales = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS)
    for line in registros_principales:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')

        # localizar el índice del número (entero) porque el nombre puede tener comas
        num_index = -1
        for i in range(2, len(parts)):
            if parts[i].strip().isdigit():
                num_index = i
                break
        if num_index == -1 or num_index + 2 >= len(parts):
            continue

        nombre = ",".join(parts[1:num_index]).strip()
        numero = parts[num_index].strip()
        pwd    = parts[num_index + 1].strip()
        code   = parts[num_index + 2].strip()

        if numero == numero_registro_input and pwd == password_input:
            return True, (nombre, code)
    
    # Si no se encontró en principal, buscar en pendientes (para registros offline)
    registros_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
    for line in registros_pendientes:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')

        # localizar el índice del número (entero) porque el nombre puede tener comas
        num_index = -1
        for i in range(2, len(parts)):
            if parts[i].strip().isdigit():
                num_index = i
                break
        if num_index == -1 or num_index + 2 >= len(parts):
            continue

        nombre = ",".join(parts[1:num_index]).strip()
        numero = parts[num_index].strip()
        pwd    = parts[num_index + 1].strip()
        code   = parts[num_index + 2].strip()

        if numero == numero_registro_input and pwd == password_input:
            return True, (nombre, code)
    
    return False, None

def check_credentials(numero_registro_input, password_input):
    # PRIMERO: Verificar si el usuario está en la lista de bajas pendientes
    if check_user_in_pending_deletions(numero_registro_input):
        write_log("AUTH_BLOCKED", f"Acceso denegado - Usuario {numero_registro_input} está en lista de bajas pendientes")
        return False, None
    
    # Intentar MySQL primero
    ok, user = check_credentials_from_mysql(numero_registro_input, password_input)
    if ok is True:
        return True, user
    if ok is None:
        # MySQL caído -> TXT
        result = check_credentials_from_txt_encrypted(numero_registro_input, password_input)
        return result
    # ok == False -> usuario no encontrado en MySQL, probar TXT como compatibilidad/offline
    result = check_credentials_from_txt_encrypted(numero_registro_input, password_input)
    return result

# ============================== OBTENER NÚMERO DE OPERADOR ==============================
def get_operator_number_from_mysql(nombre_operador):
    """Busca el número de operador en MySQL usando el nombre"""
    conn = _mysql_get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT Numero FROM registered_personnel WHERE Nombre=%s",
            (nombre_operador,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return str(row[0])
        return None
    except Exception as e:
        write_log("ERROR", f"Error obteniendo número de operador desde MySQL: {e}")
        try:
            cur.close(); conn.close()
        except:
            pass
        return None

def get_operator_number_from_txt(nombre_operador):
    """Busca el número de operador en TXT cifrado usando el nombre"""
    # Buscar en archivo principal
    registros_principales = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS)
    for line in registros_principales:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        
        # localizar el índice del número (entero) porque el nombre puede tener comas
        num_index = -1
        for i in range(2, len(parts)):
            if parts[i].strip().isdigit():
                num_index = i
                break
        if num_index == -1:
            continue
            
        nombre = ",".join(parts[1:num_index]).strip()
        numero = parts[num_index].strip()
        
        if nombre == nombre_operador:
            return numero
    
    # Si no se encontró en principal, buscar en pendientes
    registros_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
    for line in registros_pendientes:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        
        # localizar el índice del número (entero) porque el nombre puede tener comas
        num_index = -1
        for i in range(2, len(parts)):
            if parts[i].strip().isdigit():
                num_index = i
                break
        if num_index == -1:
            continue
            
        nombre = ",".join(parts[1:num_index]).strip()
        numero = parts[num_index].strip()
        
        if nombre == nombre_operador:
            return numero
    
    return None

def get_operator_number():
    """
    Obtiene el número del operador loggeado.
    Primero intenta obtenerlo del login guardado, luego de MySQL, luego de TXT
    """
    # Si ya tenemos el número del login, usarlo
    if logged_in_user_number_global:
        return logged_in_user_number_global
    
    # Si no tenemos número pero sí nombre, buscarlo
    if logged_in_user_name_global:
        # Intentar MySQL primero
        numero = get_operator_number_from_mysql(logged_in_user_name_global)
        if numero:
            return numero
        
        # Si MySQL no funciona, buscar en TXT
        numero = get_operator_number_from_txt(logged_in_user_name_global)
        if numero:
            return numero
    
    return "N/A"

# ============================== PLC / HEARTBEAT =============================
def connect_to_plc():
    global plc_client
    if plc_client is None:
        plc_client = snap7.client.Client()
    try:
        plc_client.connect(PLC_IP, RACK, SLOT)
        return plc_client.get_connected()
    except Exception:
        return False

def _hb_write(val: int):
    """Escribe 0x01/0x00 en DBB1 (DBX1.0=bit0) sin afectar otros bits."""
    try:
        if plc_client and plc_client.get_connected():
            # Leer solo el byte del heartbeat (DBB1), no DBB0
            data = bytearray(plc_client.db_read(DB_NUMBER, HEARTBEAT_BYTE_OFFSET, 1))
            if val: 
                data[0] |= 0x01   # set bit0 -> 1
            else:
                data[0] &= 0xFE   # clear bit0 -> 0
            plc_client.db_write(DB_NUMBER, HEARTBEAT_BYTE_OFFSET, data)
    except Exception as e:
        write_log("ERROR", f"Heartbeat write error: {e}")

def heartbeat_start_of_cycle():
    # Requisito: comenzar escribiendo 1
    _hb_write(1)

def heartbeat_end_of_cycle():
    # Al terminar el ciclo, dejar en 0
    time.sleep(HB_PULSE_SECONDS)  # asegura detección del 1
    _hb_write(0)
    #print("Se envío 0")

def disconnect_from_plc():
    global plc_client
    try:
        if plc_client and plc_client.get_connected():
            _hb_write(0)  # garantizar 0 al desconectar
            plc_client.disconnect()
    except Exception:
        pass

def initialize_plc_signals():
    try:
        # Asegurar que Log_Status esté en False al iniciar
        set_log_status_in_plc(False)
        
        # Inicializar esquema y tabla de trazabilidad en MySQL
        _mysql_init_schema()
        _mysql_init_traceability_table()
        
    except Exception as e:
        write_log("ERROR", f"Error inicializando señales PLC: {e}")

def write_serial_to_plc(serial_number):
    global plc_client
    try:
        if not plc_client or not plc_client.get_connected():
            if not connect_to_plc():
                return False, "No se pudo conectar al PLC"
        serial_str = str(serial_number)[:32]  # Truncar a 32 caracteres máximo
        
        # Crear bytearray para STRING[32]: 1 byte longitud + hasta 32 bytes datos + 1 byte reservado
        string_data = bytearray(34)  # STRING[32] = 34 bytes total
        string_data[0] = 32  # Longitud máxima declarada
        string_data[1] = len(serial_str)  # Longitud actual
        
        # Copiar los datos del string
        for i, char in enumerate(serial_str):
            string_data[2 + i] = ord(char)
        
        # Escribir al PLC en DB17, offset 150
        plc_client.db_write(DB_NUMBER, 150, string_data)
        return True, "Serial escrito correctamente al PLC"
        
    except Exception as e:
        return False, f"Error al escribir serial al PLC: {e}"
    
    #================== SERIAL NUMBER GUARDADO ======================

def save_serial_to_csv(serial_number):
    try:
        os.makedirs(os.path.dirname(SERIAL_NUMBER_LOG_PATH), exist_ok=True)
        key = load_key()
        if not key:
            return False, "Error: No se pudo cargar la clave de cifrado para el serial"
        
        fernet = Fernet(key)
        csv_content = f"{serial_number}"
        encrypted_content = fernet.encrypt(csv_content.encode("utf-8"))
        with open(SERIAL_NUMBER_LOG_PATH, "wb") as f:
            f.write(encrypted_content)

        # --- Actualizar archivo sin cifrar SOLO como apoyo visual ---
        try:
            os.makedirs(os.path.dirname(UNENCRYPTED_SERIAL_NUMBER_PATH), exist_ok=True)
            with open(UNENCRYPTED_SERIAL_NUMBER_PATH, "w", encoding="utf-8", newline='') as f2:
                f2.write(f"{serial_number}\n")
        except Exception as e2:
            write_log("ERROR", f"Error al escribir archivo de serial sin cifrar (visual): {e2}")
        
        return True, f"Serial cifrado guardado localmente"
        
    except Exception as e:
        return False, f"Error al guardar serial cifrado: {e}"

def read_serial_from_csv():
    try:
        if not os.path.exists(SERIAL_NUMBER_LOG_PATH):
            return None, "Archivo de serial no encontrado"
        key = load_key()
        if not key:
            return None, "Error: No se pudo cargar la clave de cifrado para leer el serial"
        fernet = Fernet(key)
        with open(SERIAL_NUMBER_LOG_PATH, "rb") as f:
            encrypted_content = f.read()
        
        # Descifrar el contenido
        decrypted_content = fernet.decrypt(encrypted_content).decode("utf-8")    
        return decrypted_content.strip(), "Serial leído correctamente"
        
    except Exception as e:
        return None, f"Error al leer serial cifrado: {e}"

    # ================== CAT NUMBER GUARDADO =================
def save_cat_number(cat_number):
    try:
        os.makedirs(os.path.dirname(CAT_NUMBER_LOG_PATH), exist_ok=True)
        key = load_key()
        if not key:
            return False, "Error: No se pudo cargar la clave de cifrado para el CAT_NUMBER"
        fernet = Fernet(key)
        encrypted_content = fernet.encrypt(str(cat_number).encode("utf-8"))
        with open(CAT_NUMBER_LOG_PATH, "wb") as f:
            f.write(encrypted_content)
        # Guardar archivo sin cifrar solo para visualización
        try:
            with open(UNENCRYPTED_CAT_NUMBER_PATH, "w", encoding="utf-8", newline='') as f2:
                f2.write(f"{cat_number}\n")
        except Exception as e2:
            write_log("ERROR", f"Error al escribir archivo de CAT_NUMBER sin cifrar (visual): {e2}")
        return True, "CAT_NUMBER guardado correctamente"
    except Exception as e:
        return False, f"Error al guardar CAT_NUMBER: {e}"

def read_cat_number():
    try:
        if not os.path.exists(CAT_NUMBER_LOG_PATH):
            return None, "Archivo de CAT_NUMBER no encontrado"
        key = load_key()
        if not key:
            return None, "Error: No se pudo cargar la clave de cifrado para leer el CAT_NUMBER"
        fernet = Fernet(key)
        with open(CAT_NUMBER_LOG_PATH, "rb") as f:
            encrypted_content = f.read()
        decrypted_content = fernet.decrypt(encrypted_content).decode("utf-8")
        return decrypted_content.strip(), "CAT_NUMBER leído correctamente"
    except Exception as e:
        return None, f"Error al leer CAT_NUMBER cifrado: {e}"


# ===================== ACTUALIZACIÓN GUI (desde el hilo) ====================
def update_gui_plc_status(is_connected, plc_internal_status_byte, product_id_value, product_counter_value, product_nok_counter_value, product_height_value,
                          meas2_value, meas3_value, meas4_value, add_info1_value, add_info2_value, serial_number_value, current_cat_number):
    global plc_status_text_var, plc_connection_indicator_canvas, plc_connection_indicator_oval_id, plc_last_update_label_var, plc_error_label_widget, plc_status_label_widget
    global product_id_var, product_counter_var, product_nok_counter_var, product_read_time_display_var, product_height_var, last_product_time
    global measurement2_var, measurement3_var, measurement4_var, additional_info1_var, additional_info2_var, serial_number_var

    if not (plc_status_text_var and plc_connection_indicator_canvas and plc_connection_indicator_oval_id and
            plc_last_update_label_var and plc_error_label_widget and plc_status_label_widget):
        return

    current_time_display = datetime.now()
    plc_last_update_label_var.set(current_time_display.strftime("%H:%M:%S"))

    current_plc_status = is_connected and (plc_internal_status_byte == 1)

    global last_plc_connection_status, plc_disconnection_time
    if not current_plc_status and last_plc_connection_status:
        plc_disconnection_time = datetime.now()
    elif current_plc_status and not last_plc_connection_status:
        plc_disconnection_time = None
    last_plc_connection_status = current_plc_status

    if current_plc_status:
        status_text = "PLC STATUS: CONECTADO!"
        light_color = COLOR_PLC_CONNECTED
        text_color = COLOR_SUCCESS_GREEN
        if plc_error_label_widget.winfo_ismapped():
            plc_error_label_widget.grid_remove()
    else:
        status_text = "PLC STATUS: DESCONECTADO"
        light_color = COLOR_DANGER_RED
        text_color = COLOR_DANGER_RED
        plc_error_label_widget.config(text="ADVERTENCIA: NO HAY CONEXIÓN CON EL PLC", fg=COLOR_PLC_WARNING)
        if not plc_error_label_widget.winfo_ismapped():
            plc_error_label_widget.grid()

    plc_status_text_var.set(status_text)
    plc_status_label_widget.config(fg=text_color)
    plc_connection_indicator_canvas.itemconfig(plc_connection_indicator_oval_id, fill=light_color, outline=light_color)

    if (product_id_var is not None and product_counter_var is not None and product_nok_counter_var is not None and
        product_read_time_display_var is not None and product_height_var is not None and
        serial_number_var is not None):

        product_id_var.set(f"Número de parte: {product_id_value}")
        product_counter_var.set(f"Piezas: {product_counter_value}")
        product_nok_counter_var.set(f"Piezas NOK: {product_nok_counter_value}")
        product_height_var.set(f"Altura: {product_height_value:.2f} mm" if isinstance(product_height_value, (int, float)) else "Altura: N/A")
        serial_number_var.set(f"Número Serial: {serial_number_value}")

        if last_product_time:
            time_elapsed = current_time_display - last_product_time
            minutes, seconds = divmod(int(time_elapsed.total_seconds()), 60)
            product_read_time_display_var.set(f"Tiempo: {minutes:02}:{seconds:02}")
        else:
            product_read_time_display_var.set("Tiempo: 00:00")

    if (measurement2_var is not None and measurement3_var is not None and
        measurement4_var is not None and additional_info1_var is not None and
        additional_info2_var is not None):

        measurement2_var.set(f" {meas2_value:.2f} mm" if isinstance(meas2_value, (int, float)) else f"Medición 2: {meas2_value}")
        measurement3_var.set(f" {meas3_value:.2f} mm" if isinstance(meas3_value, (int, float)) else f"Medición 3: {meas3_value}")
        measurement4_var.set(f" {meas4_value:.2f} mm" if isinstance(meas4_value, (int, float)) else f"Medición 4: {meas4_value}")
        
        # Mostrar CAT_NUMBER en Información Adicional 1 si está disponible
        if current_cat_number:
            additional_info1_var.set(f"CAT_NUMBER: {current_cat_number}")
        else:
            additional_info1_var.set(f" {add_info1_value}")
            
        additional_info2_var.set(f" {add_info2_value}")

# ======================== HILO DE MONITOREO DEL PLC =========================
def plc_monitoring_loop_logic():
    global plc_client, last_read_product_counter, last_product_time

    plc_is_connected = False
    plc_connected_byte_value = -1

    global plc_client, last_read_product_counter, last_product_time
    global last_laser_mark_done_state

    plc_is_connected = False
    plc_connected_byte_value = -1
    product_id_value = "N/A"
    product_counter_value = "N/A"
    product_nok_counter_value = "N/A"
    product_height_value = "N/A"
    meas2_value = "N/A"
    meas3_value = "N/A"
    meas4_value = "N/A"
    add_info1_value = "N/A"
    add_info2_value = "N/A"
    serial_number_value = "N/A"
    current_cat_number = ""  # Variable para almacenar CAT_NUMBER actual

    if not plc_client or not plc_client.get_connected():
        plc_is_connected = connect_to_plc()
    else:
        plc_is_connected = True

    if plc_is_connected:
        try:
            # --- INICIO DEL CICLO: escribe 1 ---
            heartbeat_start_of_cycle()

            data = plc_client.db_read(DB_NUMBER, 0, 300)

            plc_connected_byte_value = snap7.util.get_byte(data, 4)
            product_id_value         = snap7.util.get_string(data, 6).strip('\x00')
            product_height_value     = snap7.util.get_real(data, 126)
            meas2_value              = snap7.util.get_real(data, 130)
            meas3_value              = snap7.util.get_real(data, 134)
            meas4_value              = snap7.util.get_real(data, 138)
            current_product_counter  = snap7.util.get_dint(data, 142)
            current_nok_counter      = snap7.util.get_dint(data, 146)
            serial_number_value      = snap7.util.get_string(data, 150).strip('\x00')
            add_info1_value          = snap7.util.get_string(data, 218).strip('\x00')
            add_info2_value          = snap7.util.get_string(data, 252).strip('\x00')

            # Leer el bit Laser Mark Done (DB17, 0.1)
            laser_mark_done = (data[0] & 0x02) != 0  # 0x02 = 00000010b, bit 1

            # Inicializar estado anterior si no existe
            if 'last_laser_mark_done_state' not in globals():
                last_laser_mark_done_state = False

            # Flanco de subida: de 0 a 1
            if not last_laser_mark_done_state and laser_mark_done:
                last_product_time = datetime.now()

                # --- Buscar y escribir CAT_NUMBER al PLC antes de leerlo ---
                def buscar_cat_number(numero_parte):
                    # 1. Buscar en MySQL
                    try:
                        conn = _mysql_get_conn()
                        if conn:
                            cur = conn.cursor()
                            cur.execute("SELECT numero_cat FROM registered_parts WHERE numero_parte=%s", (numero_parte,))
                            row = cur.fetchone()
                            cur.close(); conn.close()
                            if row and row[0]:
                                return str(row[0])
                    except Exception:
                        pass
                    # 2. Buscar en TXT cifrado
                    try:
                        from cryptography.fernet import Fernet
                        registros = leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES)
                        for line in registros:
                            parts = line.strip().split(',')
                            if len(parts) >= 4 and parts[2].strip() == numero_parte:
                                return parts[3].strip()
                    except Exception:
                        pass
                    return ""

                # Buscar el CAT_NUMBER usando el número de parte (product_id_value)
                cat_number = buscar_cat_number(product_id_value)

                # Escribir CAT_NUMBER al PLC (DB17, offset 184, STRING[32])
                def write_cat_number_to_plc(cat_number):
                    global plc_client
                    try:
                        if not plc_client or not plc_client.get_connected():
                            if not connect_to_plc():
                                return False
                        cat_str = str(cat_number)[:32]
                        string_data = bytearray(34)
                        string_data[0] = 32
                        string_data[1] = len(cat_str)
                        for i, char in enumerate(cat_str):
                            string_data[2 + i] = ord(char)
                        plc_client.db_write(DB_NUMBER, 184, string_data)
                        return True
                    except Exception as e:
                        write_log("ERROR", f"Error al escribir CAT_NUMBER al PLC: {e}")
                        return False

                write_cat_number_to_plc(cat_number)

                # Leer el bit CAT Request (DB17, 0.4)
                def read_cat_request_bit():
                    try:
                        data_bit = plc_client.db_read(DB_NUMBER, 0, 1)
                        byte0 = data_bit[0]
                        return (byte0 & 0x10) != 0  # 0x10 = 00010000b, bit 4
                    except Exception as e:
                        write_log("ERROR", f"Error al leer CAT Request: {e}")
                        return False

                cat_request = read_cat_request_bit()

                # Leer el CAT_NUMBER del PLC (DB17, offset 184, STRING[32])
                def read_cat_number_from_plc():
                    try:
                        data_cat = plc_client.db_read(DB_NUMBER, 184, 34)
                        #write_log("DEBUG", f"Contenido crudo de data_cat: {list(data_cat)}")
                        # Decodificación manual de STRING S7
                        max_len = data_cat[0]
                        real_len = data_cat[1]
                        cat_number = data_cat[2:2+real_len].decode('latin-1')
                        return cat_number.strip('\x00')
                    except Exception as e:
                        write_log("ERROR", f"Error al leer CAT_NUMBER del PLC: {e}")
                        return ""

                # Guardar en CSV individual y MySQL según el valor de CAT Request
                try:
                    # Separar fecha y hora
                    current_datetime = datetime.now()
                    current_date = current_datetime.strftime("%Y-%m-%d")
                    current_time = current_datetime.strftime("%H:%M:%S")
                    
                    # Obtener número del operador
                    operator_number = get_operator_number()
                    
                    # Calcular tiempo entre productos (antes de actualizar last_product_time)
                    tiempo_entre_productos = get_time_between_products()
                    
                    # Actualizar contador diario de piezas
                    piezas_marcadas_dia = update_daily_piece_counter()
                    
                    # Determinar CAT_NUMBER según CAT Request
                    if cat_request:
                        cat_number_plc = read_cat_number_from_plc()
                        current_cat_number = cat_number_plc  # Guardar para mostrar en GUI
                        write_log("INFO", f"CAT_NUMBER leído para CSV: '{cat_number_plc}'")
                        
                        # Crear archivo CSV individual con CAT_NUMBER
                        success, message = save_individual_product_csv(
                            current_date, current_time, operator_number, product_id_value,
                            serial_number_value, product_height_value, cat_number_plc
                        )
                        
                        # Guardar en MySQL con CAT_NUMBER y nuevos campos
                        insert_traceability_data_to_mysql(
                            current_date, current_time, operator_number, product_id_value,
                            serial_number_value, product_height_value, cat_number_plc, 
                            tiempo_entre_productos, piezas_marcadas_dia
                        )
                    else:
                        # CAT Request inactivo: crear CSV y MySQL sin CAT_NUMBER
                        current_cat_number = ""  # Sin CAT_NUMBER para mostrar en GUI
                        
                        # Crear archivo CSV individual sin CAT_NUMBER
                        success, message = save_individual_product_csv(
                            current_date, current_time, operator_number, product_id_value,
                            serial_number_value, product_height_value, ""
                        )
                        
                        # Guardar en MySQL sin CAT_NUMBER pero con nuevos campos
                        insert_traceability_data_to_mysql(
                            current_date, current_time, operator_number, product_id_value,
                            serial_number_value, product_height_value, "", 
                            tiempo_entre_productos, piezas_marcadas_dia
                        )
                    
                    if not success:
                        write_log("ERROR", f"Error guardando CSV individual: {message}")
                    else:
                        # Log informativo sobre los nuevos datos calculados
                        tiempo_info = f"{tiempo_entre_productos}s" if tiempo_entre_productos else "Primera pieza"
                        write_log("INFO", f"Pieza marcada - Tiempo desde anterior: {tiempo_info}, Piezas del día: {piezas_marcadas_dia}")
                        
                except Exception as e:
                    write_log("ERROR", f"Error al procesar guardado de datos: {e}")

            # Actualizar estado anterior
            last_laser_mark_done_state = laser_mark_done

            # El contador de productos solo se actualiza para mostrarlo
            product_counter_value = current_product_counter
            product_nok_counter_value = current_nok_counter

            # --- FIN DEL CICLO: dejar 0 ---
            heartbeat_end_of_cycle()

        except Exception as e:
            print(f"DEBUG HILO: Error PLC (DB{DB_NUMBER}): {e}")
            # asegurar 0 ante error
            try: heartbeat_end_of_cycle()
            except: pass
            plc_connected_byte_value = -1
            product_id_value = "ERROR"
            product_counter_value = "ERROR"
            product_nok_counter_value = "ERROR"
            product_height_value = "ERROR"
            meas2_value = "ERROR"
            meas3_value = "ERROR"
            meas4_value = "ERROR"
            add_info1_value = "ERROR"
            add_info2_value = "ERROR"
            serial_number_value = "ERROR"
            current_cat_number = ""
            plc_is_connected = False
            disconnect_from_plc()

    return (plc_is_connected, plc_connected_byte_value, product_id_value, product_counter_value, product_nok_counter_value, product_height_value,
            meas2_value, meas3_value, meas4_value, add_info1_value, add_info2_value, serial_number_value, current_cat_number)

def plc_monitoring_thread_runner():
    while not stop_monitoring_event.is_set():
        values = plc_monitoring_loop_logic()
        (plc_is_connected, plc_internal_status_byte_from_plc, product_id_value, product_counter_value, product_nok_counter_value, product_height_value,
         meas2_value, meas3_value, meas4_value, add_info1_value, add_info2_value, serial_number_value, current_cat_number) = values

        if root_machine_app and root_machine_app.winfo_exists():
            try:
                root_machine_app.after(0, lambda: update_gui_plc_status(
                    plc_is_connected, plc_internal_status_byte_from_plc, product_id_value, product_counter_value, product_nok_counter_value, product_height_value,
                    meas2_value, meas3_value, meas4_value, add_info1_value, add_info2_value, serial_number_value, current_cat_number
                ))
            except tk.TclError:
                break
            except Exception:
                break
        else:
            break

        time.sleep(READ_INTERVAL_SECONDS)
    disconnect_from_plc()

def start_monitoring_thread():
    global monitoring_thread, stop_monitoring_event
    stop_monitoring_event.clear()
    monitoring_thread = threading.Thread(target=plc_monitoring_thread_runner, daemon=True)
    monitoring_thread.start()

def stop_monitoring_thread():
    global stop_monitoring_event, monitoring_thread
    if monitoring_thread and monitoring_thread.is_alive():
        stop_monitoring_event.set()
        monitoring_thread.join(timeout=5)

# ============================= AUTENTICACIÓN GUI ============================
def attempt_machine_login(parent_root, registro_var, password_var, entry_registro_login):
    global login_successful_machine, logged_in_user_name_global, logged_in_laser_code_global, logged_in_datetime_global, logged_in_user_number_global
    numero_registro = registro_var.get().strip()
    password = password_var.get().strip()

    if not numero_registro or not password:
        messagebox.showwarning("Campos Vacíos", "Por favor ingresa tu número de registro y contraseña.", parent=parent_root)
        return

    success, user_data = check_credentials(numero_registro, password)
    if success:
        login_successful_machine = True
        logged_in_user_name_global  = user_data[0]
        logged_in_laser_code_global = user_data[1]
        logged_in_user_number_global = numero_registro  # Guardar número de usuario
        logged_in_datetime_global   = datetime.now()
        write_log("LOGIN", f"'{logged_in_user_name_global}' ({logged_in_laser_code_global}) ha iniciado sesión.")

        # Escribir Log_Status = 1 en PLC
        set_log_status_in_plc(True)

        try:
            os.makedirs(os.path.dirname(LASER_CODE_FILE_PATH), exist_ok=True)
            with open(LASER_CODE_FILE_PATH, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                #writer.writerow(["Laser_Code"])
                writer.writerow([logged_in_laser_code_global])
        except Exception as e:
            messagebox.showerror("Error de Archivo", f"No se pudo crear el archivo de código láser:\n{e}", parent=parent_root)
            write_log("ERROR", f"Error al crear archivo de código láser: {e}")

        show_logged_in_screen(parent_root, logged_in_user_name_global, logged_in_laser_code_global)
    else:
        # Escribir Log_Status = 0 en PLC si login falla
        set_log_status_in_plc(False)
        messagebox.showerror("Error de Acceso", "Número de registro o contraseña incorrectos.", parent=parent_root)
        registro_var.set(""); password_var.set(""); entry_registro_login.focus_set()
# ========================== CIERRE DE SESIÓN ==============================
def logout_machine_user():
    global login_successful_machine, logged_in_user_name_global, logged_in_laser_code_global, logged_in_datetime_global, logged_in_user_number_global
    login_successful_machine = False
    logged_in_user_name_global = None
    logged_in_laser_code_global = None
    logged_in_user_number_global = None
    logged_in_datetime_global = None
    # Escribir Log_Status = 0 en PLC
    set_log_status_in_plc(False)
    write_log("LOGOUT", "Usuario ha cerrado sesión.")
    # Aquí puedes agregar lógica para volver a la pantalla de login si lo deseas

# ================================ MENÚS / UI ================================
def show_serial_authorization_window():
    """Ventana de autorización para reestablecer número serial"""
    auth_window = tk.Toplevel(root_machine_app)
    set_custom_icon(auth_window)
    auth_window.title("Autorización Requerida")
    auth_window.geometry("380x250")
    auth_window.configure(bg=COLOR_BACKGROUND_PRIMARY)
    auth_window.resizable(False, False)
    auth_window.grab_set()  # Modal

    # Centrar la ventana
    auth_window.update_idletasks()
    x = root_machine_app.winfo_x() + (root_machine_app.winfo_width() // 2) - (auth_window.winfo_width() // 2)
    y = root_machine_app.winfo_y() + (root_machine_app.winfo_height() // 2) - (auth_window.winfo_height() // 2)
    auth_window.geometry(f"+{x}+{y}")

    # Frame principal
    main_frame = tk.Frame(auth_window, bg=COLOR_BACKGROUND_SECONDARY, bd=2, relief="raised")
    main_frame.pack(fill="both", expand=True, padx=20, pady=20)

    # Título
    tk.Label(main_frame, text="Acceso Autorizado Requerido", 
             font=FONT_SECTION_HEADER, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE).pack(pady=(10, 10))

    # Frame para entrada
    input_frame = tk.Frame(main_frame, bg=COLOR_BACKGROUND_SECONDARY)
    input_frame.pack(pady=0, fill="x", padx=20)

    tk.Label(input_frame, text="Usuario:", font=FONT_LABEL, 
             bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).pack(anchor="w", pady=(5,2))
    
    user_var = tk.StringVar()
    entry_user = tk.Entry(input_frame, textvariable=user_var, font=FONT_ENTRY,
                         bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_PRIMARY, insertbackground=COLOR_TEXT_PRIMARY)
    entry_user.pack(fill="x", pady=(0,10))
    entry_user.focus_set()

    tk.Label(input_frame, text="Contraseña:", font=FONT_LABEL, 
             bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).pack(anchor="w", pady=(0,2))
    
    pass_var = tk.StringVar()
    entry_pass = tk.Entry(input_frame, textvariable=pass_var, font=FONT_ENTRY, show="*",
                         bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_PRIMARY, insertbackground=COLOR_TEXT_PRIMARY)
    entry_pass.pack(fill="x", pady=(0,10))

    def authorize_access():
        username = user_var.get().strip()
        password = pass_var.get().strip()
        
        if not username or not password:
            messagebox.showwarning("Campos Vacíos", "Por favor ingresa usuario y contraseña.", parent=auth_window)
            return
        
        # Verificar credenciales específicas para reestablecer serial
        if username == "ADMIN" and password == "PASSWORD":
            write_log("AUTH", f"Acceso autorizado para reestablecer serial - Usuario: {username}")
            auth_window.destroy()
            show_serial_initialization_window()
        else:
            write_log("AUTH", f"Intento de acceso fallido para reestablecer serial - Usuario: {username}")
            messagebox.showerror("Acceso Denegado", "Credenciales incorrectas.", parent=auth_window)
            user_var.set("")
            pass_var.set("")
            entry_user.focus_set()

    # Permitir Enter para autorizar
    auth_window.bind('<Return>', lambda event: authorize_access())
    # Permitir Escape para cerrar
    auth_window.bind('<Escape>', lambda event: auth_window.destroy())

def show_serial_initialization_window():
    """Ventana para reestablecer número serial"""
    serial_window = tk.Toplevel(root_machine_app)
    set_custom_icon(serial_window)
    serial_window.title("Reestablecer Número Serial")
    serial_window.geometry("400x200")
    serial_window.configure(bg=COLOR_BACKGROUND_PRIMARY)
    serial_window.resizable(False, False)
    
    # Evitar que se pueda minimizar
    serial_window.attributes('-toolwindow', True) 
    serial_window.grab_set() 

    # Centrar la ventana
    serial_window.update_idletasks()
    x = root_machine_app.winfo_x() + (root_machine_app.winfo_width() // 2) - (serial_window.winfo_width() // 2)
    y = root_machine_app.winfo_y() + (root_machine_app.winfo_height() // 2) - (serial_window.winfo_height() // 2)
    serial_window.geometry(f"+{x}+{y}")

    # Frame principal
    main_frame = tk.Frame(serial_window, bg=COLOR_BACKGROUND_SECONDARY, bd=2, relief="raised")
    main_frame.pack(fill="both", expand=True, padx=20, pady=20)

    # Título
    tk.Label(main_frame, text="Reestablecer Número Serial", 
             font=FONT_SECTION_HEADER, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE).pack(pady=(10, 20))

    # Frame para entrada
    input_frame = tk.Frame(main_frame, bg=COLOR_BACKGROUND_SECONDARY)
    input_frame.pack(pady=0)

    tk.Label(input_frame, text="Número serial a reestablecer (6 dígitos)", 
             font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).pack(pady=1)
    
    serial_var = tk.StringVar()
    entry_serial = tk.Entry(input_frame, textvariable=serial_var, font=FONT_ENTRY, width=30,
                           bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_PRIMARY, insertbackground=COLOR_TEXT_PRIMARY)
    entry_serial.pack(pady=1)
    entry_serial.focus_set()

    def initialize_serial():
        serial_number = serial_var.get().strip()
        if not serial_number:
            messagebox.showwarning("Campo Vacío", "Por favor ingresa un número serial.", parent=serial_window)
            return
        
        # Validar que sean exactamente 6 dígitos
        if not serial_number.isdigit():
            messagebox.showerror("Formato Inválido", "El número serial debe contener solo dígitos.", parent=serial_window)
            return
        
        if len(serial_number) != 6:
            messagebox.showerror("Formato Inválido", "El número serial debe ser de exactamente 6 dígitos.\n\nFormato válido: 000001 - 999999", parent=serial_window)
            return
        
        # Validar rango (000001 a 999999)
        serial_int = int(serial_number)
        if serial_int < 1 or serial_int > 999999:
            messagebox.showerror("Rango Inválido", "El número serial debe estar entre 000001 y 999999.", parent=serial_window)
            return
        
        # Formatear a 6 dígitos con ceros a la izquierda
        formatted_serial = f"{serial_int:06d}"
        
        # Mostrar el serial actual antes de confirmar (solo para administradores)
        try:
            current_serial, read_message = read_serial_from_csv()
            if current_serial:
                confirmation_text = f"¿Deseas reestablecer el número serial?\n\nSerial actual: {current_serial}\nNuevo serial: {formatted_serial}\n\nEsto escribirá el serial al PLC y lo guardará cifrado en el registro."
            else:
                confirmation_text = f"¿Deseas reestablecer el número serial?\n\nSerial: {formatted_serial}\n\nEsto escribirá el serial al PLC y lo guardará cifrado en el registro."
        except:
            confirmation_text = f"¿Deseas reestablecer el número serial?\n\nSerial: {formatted_serial}\n\nEsto escribirá el serial al PLC y lo guardará cifrado en el registro."
        
        # Confirmación del usuario
        result = messagebox.askyesno("Confirmar", confirmation_text, parent=serial_window)
        if result:
            # Escribir al PLC
            plc_success, plc_message = write_serial_to_plc(formatted_serial)
            
            # Guardar en CSV
            csv_success, csv_message = save_serial_to_csv(formatted_serial)
            
            # Registrar en logs
            write_log("SERIAL", f"Número serial reestablecido: {formatted_serial}")
            
            # Mostrar resultados
            if plc_success and csv_success:
                messagebox.showinfo("Éxito Completo", 
                                   f"Número serial {formatted_serial} reestablecido correctamente.\n\n" +
                                   f"✓ {plc_message}\n✓ {csv_message}", 
                                   parent=serial_window)
                write_log("SERIAL", f"Serial {formatted_serial} - PLC: OK, CSV: OK")
            else:
                error_messages = []
                if not plc_success:
                    error_messages.append(f"❌ PLC: {plc_message}")
                    write_log("ERROR", f"Serial {formatted_serial} - PLC falló: {plc_message}")
                else:
                    error_messages.append(f"✓ PLC: {plc_message}")
                
                if not csv_success:
                    error_messages.append(f"❌ CSV: {csv_message}")
                    write_log("ERROR", f"Serial {formatted_serial} - CSV falló: {csv_message}")
                else:
                    error_messages.append(f"✓ CSV: {csv_message}")
                
                messagebox.showwarning("Reestablecimiento Parcial", 
                                     f"Número serial {formatted_serial} procesado con algunos errores:\n\n" +
                                     "\n".join(error_messages), 
                                     parent=serial_window)
            
            serial_window.destroy()

    serial_window.bind('<Return>', lambda event: initialize_serial())
    serial_window.bind('<Escape>', lambda event: serial_window.destroy())

def show_logs_window():

    log_window = tk.Toplevel(root_machine_app)
    set_custom_icon(log_window)
    log_window.title("Historial de Registros")
    log_window.geometry("850x400")
    log_window.configure(bg=COLOR_BACKGROUND_PRIMARY)
    log_window.attributes('-toolwindow', True)  # No se puede minimizar
    log_window.grab_set()  # Modal

    log_window.update_idletasks()
    x = root_machine_app.winfo_x() + (root_machine_app.winfo_width() // 2) - (log_window.winfo_width() // 2)
    y = root_machine_app.winfo_y() + (root_machine_app.winfo_height() // 2) - (log_window.winfo_height() // 2)
    log_window.geometry(f"+{x}+{y}")

    text_area = scrolledtext.ScrolledText(log_window, wrap=tk.WORD, bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_LIGHT,
                                          font=FONT_LABEL, bd=0, relief="flat", padx=10, pady=10)
    text_area.pack(expand=True, fill="both", padx=10, pady=10)

    for line in read_logs():
        text_area.insert(tk.END, line + "\n")

    text_area.config(state=tk.DISABLED)
    text_area.see(tk.END)
    log_window.focus_set()  # Llevar el foco a la ventana



def show_error_logs_window():
    log_window = tk.Toplevel(root_machine_app)
    set_custom_icon(log_window)
    log_window.title("Historial de Logs de Errores")
    log_window.geometry("850x400")
    log_window.configure(bg=COLOR_BACKGROUND_PRIMARY)
    log_window.attributes('-toolwindow', True)  # No se puede minimizar
    log_window.grab_set()  # Modal


    log_window.update_idletasks()
    x = root_machine_app.winfo_x() + (root_machine_app.winfo_width() // 2) - (log_window.winfo_width() // 2)
    y = root_machine_app.winfo_y() + (root_machine_app.winfo_height() // 2) - (log_window.winfo_height() // 2)
    log_window.geometry(f"+{x}+{y}")
    log_window.focus_set()  # Llevar el foco a la ventana

    text_area = scrolledtext.ScrolledText(log_window, wrap=tk.WORD, bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_LIGHT,
                                          font=FONT_LABEL, bd=0, relief="flat", padx=10, pady=10)
    text_area.pack(expand=True, fill="both", padx=10, pady=10)

    for line in read_error_logs():
        text_area.insert(tk.END, line + "\n")
    text_area.config(state=tk.DISABLED)
    text_area.see(tk.END)

def show_system_info():
    info = f"Conexión PLC:\n  IP: {PLC_IP}\n  DB de Lectura: DB{DB_NUMBER}\n  Rack: {RACK}\n  Slot: {SLOT}"
    messagebox.showinfo("Información del Sistema", info, parent=root_machine_app)

def show_network_info():
    global last_plc_connection_status, plc_disconnection_time
    if last_plc_connection_status:
        status_message = "El PLC está CONECTADO actualmente."
    else:
        if plc_disconnection_time:
            dt = datetime.now() - plc_disconnection_time
            minutes, seconds = divmod(int(dt.total_seconds()), 60)
            hours, minutes = divmod(minutes, 60)
            status_message = (f"El PLC está DESCONECTADO.\n"
                              f"Tiempo desde la última desconexión: {hours:02}h {minutes:02}m {seconds:02}s")
        else:
            status_message = "El PLC está DESCONECTADO (no se ha registrado una conexión previa para calcular el tiempo)."
    messagebox.showinfo("Estado de Conexión de Red", status_message, parent=root_machine_app)

def save_current_data_to_file():
    global root_machine_app, plc_status_text_var, plc_last_update_label_var, product_id_var, product_counter_var, product_height_var, product_read_time_display_var, plc_disconnection_time
    global logged_in_user_name_global, logged_in_laser_code_global, logged_in_datetime_global
    global measurement2_var, measurement3_var, measurement4_var, additional_info1_var, additional_info2_var, serial_number_var

    if not login_successful_machine:
        messagebox.showwarning("Acceso Denegado", "Debes iniciar sesión para descargar datos.", parent=root_machine_app)
        return

    now = datetime.now()
    formatted_download_time = now.strftime('%Y-%m-%d %H:%M:%S')

    user_name = logged_in_user_name_global or 'N/A'
    laser_code = logged_in_laser_code_global or 'N/A'
    login_datetime = logged_in_datetime_global.strftime('%Y-%m-%d %H:%M:%S') if logged_in_datetime_global else 'N/A'

    plc_status = plc_status_text_var.get() if plc_status_text_var else 'N/A'
    plc_last_update = plc_last_update_label_var.get() if plc_last_update_label_var else 'N/A'

    time_since_disconnection_str = "N/A"
    if not last_plc_connection_status and plc_disconnection_time:
        delta = now - plc_disconnection_time
        minutes, seconds = divmod(int(delta.total_seconds()), 60)
        hours, minutes = divmod(minutes, 60)
        time_since_disconnection_str = f"{hours:02}h {minutes:02}m {seconds:02}s"

    product_id = product_id_var.get().replace("Número de Parte: ", "") if product_id_var else 'N/A'
    product_counter = product_counter_var.get().replace("Piezas: ", "") if product_counter_var else 'N/A'
    product_height = product_height_var.get().replace("Altura: ", "").replace(" mm", "") if product_height_var else 'N/A'
    product_read_time = product_read_time_display_var.get().replace("Tiempo desde la última pieza: ", "") if product_read_time_display_var else 'N/A'
    serial_number = (serial_number_var.get()
                     .replace("Número Serial: ", "")
                     .replace("Número Serial : ", "")
                     .replace("Serial Number: ", "")) if serial_number_var else 'N/A'

    meas2 = measurement2_var.get().replace("Medición 2: ", "").replace(" mm", "") if measurement2_var else 'N/A'
    meas3 = measurement3_var.get().replace("Medición 3: ", "").replace(" mm", "") if measurement3_var else 'N/A'
    meas4 = measurement4_var.get().replace("Medición 4: ", "").replace(" mm", "") if measurement4_var else 'N/A'
    add_info1 = additional_info1_var.get().replace("Información Adicional 1: ", "") if additional_info1_var else 'N/A'
    add_info2 = additional_info2_var.get().replace("Información Adicional 2: ", "") if additional_info2_var else 'N/A'

    file_path = filedialog.asksaveasfilename(
        parent=root_machine_app,
        defaultextension=".txt",
        filetypes=[("Archivos de Texto", "*.txt"), ("Archivos CSV", "*.csv"), ("Todos los Archivos", "*.*")],
        title="Guardar Datos Actuales"
    )
    if not file_path:
        return

    try:
        if file_path.lower().endswith('.csv'):
            headers = [
                "Download_Timestamp", "User_Name", "Laser_Code", "Login_Timestamp",
                "PLC_Status", "PLC_Last_Update", "PLC_Disconnection_Time_Elapsed",
                "Product_ID", "Product_Counter", "Product_Height_mm", "Time_Since_Last_Product", "Serial_Number",
                "Measurement_2_mm", "Measurement_3_mm", "Measurement_4_mm", "Additional_Info_1", "Additional_Info_2"
            ]
            data_row = [
                formatted_download_time, user_name, laser_code, login_datetime,
                plc_status, plc_last_update, time_since_disconnection_str,
                product_id, product_counter, product_height, product_read_time, serial_number,
                meas2, meas3, meas4, add_info1, add_info2
            ]
            with open(file_path, "w", newline='', encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                writer.writerow(data_row)
        else:
            data_to_save = []
            data_to_save.append(f"--- Datos del Panel General de Control ---")
            data_to_save.append(f"Fecha y Hora de Descarga: {formatted_download_time}")
            data_to_save.append(f"---------------------------------------------")
            if logged_in_user_name_global and logged_in_laser_code_global and logged_in_datetime_global:
                data_to_save.append(f"Detalles de la Sesión:")
                data_to_save.append(f"  Usuario: {user_name}")
                data_to_save.append(f"  Código Láser: {laser_code}")
                data_to_save.append(f"  Fecha y Hora de Acceso: {login_datetime}")
                data_to_save.append(f"---------------------------------------------")
            data_to_save.append(f"Estado del PLC: {plc_status}")
            data_to_save.append(f"Última Actualización del PLC: {plc_last_update}")
            data_to_save.append(f"Tiempo desde Desconexión PLC: {time_since_disconnection_str}")
            data_to_save.append(f"---------------------------------------------")
            data_to_save.append(f"Datos del Número de Parte:")
            data_to_save.append(f"  Número de Parte: {product_id}")
            data_to_save.append(f"  Contador de Piezas: {product_counter}")
            data_to_save.append(f"  Altura: {product_height} mm")
            data_to_save.append(f"  Tiempo desde la última pieza: {product_read_time}") 
            data_to_save.append(f"  Número Serial: {serial_number}")                    
            data_to_save.append(f"---------------------------------------------")
            data_to_save.append(f"Otros Datos de Medición y Adicionales (Pestaña Lectura PLC):")
            data_to_save.append(f"  Medición 2: {meas2} mm")
            data_to_save.append(f"  Medición 3: {meas3} mm")
            data_to_save.append(f"  Medición 4: {meas4} mm")
            data_to_save.append(f"  Información Adicional 1: {add_info1}")
            data_to_save.append(f"  Información Adicional 2: {add_info2}")
            data_to_save.append(f"---------------------------------------------")
            with open(file_path, "w", encoding="utf-8") as file:
                for line in data_to_save:
                    file.write(line + "\n")

        messagebox.showinfo("Guardado Exitoso", f"Los datos se han guardado en:\n{file_path}", parent=root_machine_app)
        write_log("SAVE_DATA", f"Datos actuales guardados en: {file_path}")
    except Exception as e:
        messagebox.showerror("Error al Guardar", f"No se pudo guardar el archivo:\n{e}", parent=root_machine_app)
        write_log("ERROR", f"Error al guardar datos: {e}")

def show_about():
    messagebox.showinfo("Acerca de", "PROGRAM DESIGNED FOR VSCT MEXICO", parent=root_machine_app)

def show_user_manual():
    manual_text = (
        "MANUAL DE USUARIO\n\n"
        "• Inicio de Sesión: Ingresa tu número de registro y contraseña. El sistema valida primero en MySQL y, si no es posible, en el respaldo cifrado.\n\n"
        "• Monitoreo de PLC: El sistema lee datos del PLC en tiempo real, mostrando estatus de conexión y variables clave de producción.\n\n"
        "• Guardado de Datos: Cada ciclo de producción exitoso se registra automáticamente en un archivo CSV diario y en la base de datos MySQL.\n\n"
        "• Logs y Errores: Puedes consultar el historial de registros (inicios/cierres de sesión, salidas de app) y el historial de logs de errores desde el menú. Todos los errores críticos quedan registrados automáticamente.\n\n"
        "• Serial y CAT_NUMBER: El número serial y el CAT_NUMBER se gestionan y guardan de forma segura, y pueden ser reestablecidos por usuarios autorizados.\n\n"
        "• Seguridad: El acceso está restringido a usuarios válidos y se bloquea automáticamente si el usuario está dado de baja.\n\n"
        "• Recomendación: No cierres la app abruptamente y mantén actualizados los archivos de configuración y logs para asegurar la trazabilidad.\n"
    )
    messagebox.showinfo("Manual de Usuario", manual_text, parent=root_machine_app)

def create_menu_bar(parent_root, enable_download_menu=False, enable_logs_menu=False, enable_serial_menu=False):
    global download_menu_instance, logs_menu_instance, serial_menu_instance

    menubar = tk.Menu(parent_root, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY,
                      activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    parent_root.config(menu=menubar)

    system_menu = tk.Menu(menubar, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                          activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    menubar.add_cascade(label="Sistema", menu=system_menu)
    system_menu.add_command(label="Información...", command=show_system_info)
    system_menu.add_separator()
    system_menu.add_command(label="Salir", command=on_main_window_close)

    network_menu = tk.Menu(menubar, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                           activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    menubar.add_cascade(label="Red", menu=network_menu)
    network_menu.add_command(label="Estado de Conexión...", command=show_network_info)
    network_menu.add_separator()
    download_menu = tk.Menu(network_menu, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                            activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    download_menu_instance = download_menu
    network_menu.add_cascade(label="Descargar a PC...", menu=download_menu)
    download_menu.add_command(label="Guardar Datos Actuales", command=save_current_data_to_file)
    download_menu_instance.entryconfig("Guardar Datos Actuales", state="normal" if enable_download_menu else "disabled")

    logs_menu = tk.Menu(menubar, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                        activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    menubar.add_cascade(label="Logs", menu=logs_menu)
    logs_menu.add_command(label="Ver Historial de Registros", command=show_logs_window)
    logs_menu.add_command(label="Ver Historial de Logs", command=show_error_logs_window)
    logs_menu_instance = logs_menu
    logs_menu_instance.entryconfig("Ver Historial de Registros", state="normal" if enable_logs_menu else "disabled")

    # Menú Serial
    serial_menu = tk.Menu(menubar, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                         activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    menubar.add_cascade(label="Serial", menu=serial_menu)
    serial_menu.add_command(label="Reestablecer número serial", command=show_serial_authorization_window)
    serial_menu_instance = serial_menu
    serial_menu_instance.entryconfig("Reestablecer número serial", state="normal" if enable_serial_menu else "disabled")

    help_menu = tk.Menu(menubar, tearoff=0, bg=COLOR_SUBMENU_BACKGROUND, fg=COLOR_SUBMENU_FOREGROUND,
                        activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    menubar.add_cascade(label="Ayuda", menu=help_menu)
    help_menu.add_command(label="Acerca de...", command=show_about)
    help_menu.add_separator()
    help_menu.add_command(label="Manual de Usuario", command=show_user_manual)

# =============================== PANTALLAS GUI ==============================
def show_logged_in_screen(parent_root, user_name, laser_code):
    global root_machine_app
    global plc_status_text_var, plc_connection_indicator_canvas, plc_connection_indicator_oval_id, plc_last_update_label_var, plc_error_label_widget, plc_status_label_widget
    global product_id_var, product_counter_var, product_nok_counter_var, product_read_time_display_var, product_height_var
    global measurement2_var, measurement3_var, measurement4_var, additional_info1_var, additional_info2_var, serial_number_var

    root_machine_app = parent_root
    root_machine_app.deiconify()
    root_machine_app.title("MANUAL LASERCELL")
    root_machine_app.geometry("660x850")
    root_machine_app.configure(bg=COLOR_BACKGROUND_PRIMARY)
    set_custom_icon(root_machine_app)

    for w in root_machine_app.winfo_children():
        w.destroy()

    create_menu_bar(root_machine_app, enable_download_menu=True, enable_logs_menu=True, enable_serial_menu=True)

    root_machine_app.update_idletasks()
    x = root_machine_app.winfo_screenwidth() // 2 - root_machine_app.winfo_width() // 2
    y = root_machine_app.winfo_screenheight() // 2 - root_machine_app.winfo_height() // 2
    root_machine_app.geometry(f"+{x}+{y}")

    # --- Scrollable main_content_frame ---
    main_canvas = tk.Canvas(root_machine_app, bg=COLOR_BACKGROUND_PRIMARY, highlightthickness=0)
    main_canvas.pack(side="left", fill="both", expand=True)
    x_scroll = tk.Scrollbar(root_machine_app, orient="horizontal", command=main_canvas.xview)
    x_scroll.pack(side="bottom", fill="x")
    y_scroll = tk.Scrollbar(root_machine_app, orient="vertical", command=main_canvas.yview)
    y_scroll.pack(side="right", fill="y")
    main_canvas.configure(xscrollcommand=x_scroll.set, yscrollcommand=y_scroll.set)
    main_content_frame = tk.Frame(main_canvas, bg=COLOR_BACKGROUND_PRIMARY, padx=30, pady=30)
    main_content_frame_id = main_canvas.create_window((0,0), window=main_content_frame, anchor="nw")
    def on_configure(event):
        main_canvas.configure(scrollregion=main_canvas.bbox("all"))
    main_content_frame.bind("<Configure>", on_configure)
    def on_canvas_configure(event):
        main_canvas.itemconfig(main_content_frame_id, width=event.width)
    main_canvas.bind("<Configure>", on_canvas_configure)

    # --- ESTADO DEL PLC ---
    plc_status_frame = tk.LabelFrame(main_content_frame, text="ESTADO DEL PLC", font=FONT_SECTION_HEADER,
                                     bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                     highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=30, pady=25)
    plc_status_frame.pack(pady=(0, 25), fill="x")
    plc_status_frame.grid_columnconfigure(0, weight=1)
    plc_status_frame.grid_columnconfigure(1, weight=3)

    global plc_connection_indicator_oval_id, plc_connection_indicator_canvas
    plc_connection_indicator_canvas = tk.Canvas(plc_status_frame, width=40, height=40, bg=COLOR_BACKGROUND_SECONDARY, highlightthickness=0)
    plc_connection_indicator_canvas.grid(row=0, column=0, sticky="w", padx=5, pady=2)
    plc_connection_indicator_oval_id = plc_connection_indicator_canvas.create_oval(5, 5, 35, 35, fill=COLOR_PLC_DISCONNECTED, outline=COLOR_PLC_DISCONNECTED)

    global plc_status_text_var, plc_status_label_widget, plc_last_update_label_var, plc_error_label_widget
    plc_status_text_var = tk.StringVar(value="Conectando al PLC...")
    plc_status_label_widget = tk.Label(plc_status_frame, textvariable=plc_status_text_var, font=FONT_STATUS_INFO, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY)
    plc_status_label_widget.grid(row=0, column=1, sticky="w", pady=2, padx=5)

    tk.Label(plc_status_frame, text="Última Actualización:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    plc_last_update_label_var = tk.StringVar(value=datetime.now().strftime("%H:%M:%S"))
    tk.Label(plc_status_frame, textvariable=plc_last_update_label_var, font=FONT_STATUS_INFO, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=1, column=1, sticky="w", pady=2, padx=5)

    plc_error_label_widget = tk.Label(plc_status_frame, text="", font=FONT_STATUS_ERROR, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_DANGER_RED)
    plc_error_label_widget.grid(row=2, column=0, columnspan=2, pady=(10,5), padx=5, sticky="w")
    plc_error_label_widget.grid_remove()

    # --- DETALLES DE SESIÓN ---
    user_info_frame = tk.LabelFrame(main_content_frame, text="DETALLES DE SESIÓN", font=FONT_SECTION_HEADER, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                    highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=20, pady=15)
    user_info_frame.pack(pady=(0, 25), fill="x")
    user_info_frame.grid_columnconfigure(0, weight=1)
    user_info_frame.grid_columnconfigure(1, weight=3)

    tk.Label(user_info_frame, text="Usuario:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=0, column=0, sticky="w", pady=2, padx=5)
    tk.Label(user_info_frame, text=user_name, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=0, column=1, sticky="w", pady=2, padx=5)

    tk.Label(user_info_frame, text="Fecha y Hora de Acceso:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    tk.Label(user_info_frame, text=logged_in_datetime_global.strftime("%Y-%m-%d %H:%M:%S"), font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=1, column=1, sticky="w", pady=2, padx=5)

    tk.Label(user_info_frame, text="Código Láser:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=2, column=0, sticky="w", pady=2, padx=5)
    tk.Label(user_info_frame, text=laser_code, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=2, column=1, sticky="w", pady=2, padx=5)

    # --- TABS ---
    notebook = ttk.Notebook(main_content_frame)
    notebook.pack(pady=(0, 25), fill="both", expand=True)

    style = ttk.Style()
    style.theme_use('classic')
    style.configure("TNotebook", background=COLOR_BACKGROUND_PRIMARY, borderwidth=0)
    style.configure("TNotebook.Tab", background=COLOR_BACKGROUND_SECONDARY, foreground=COLOR_TEXT_PRIMARY,
                    font=FONT_SECTION_HEADER, padding=[10, 5])
    style.map("TNotebook.Tab", background=[("selected", COLOR_BACKGROUND_TERTIARY)],
              foreground=[("selected", COLOR_TEXT_PRIMARY)])

    tab_overview = tk.LabelFrame(notebook, text="", font=FONT_SECTION_HEADER,
                                 bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                 highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=20, pady=15)
    notebook.add(tab_overview, text="GENERAL")
    tab_overview.grid_columnconfigure(0, weight=1)
    tab_overview.grid_columnconfigure(1, weight=3)

    global product_id_var, product_read_time_display_var, product_counter_var, product_nok_counter_var, product_height_var, serial_number_var
    product_id_var = tk.StringVar(value="Número de Parte: N/A")
    product_read_time_display_var = tk.StringVar(value="Tiempo: 00:00")
    product_counter_var = tk.StringVar(value="Piezas: 0")
    product_nok_counter_var = tk.StringVar(value="Piezas NOK: 0")
    product_height_var = tk.StringVar(value="Altura: N/A")
    serial_number_var = tk.StringVar(value="Número Serial: N/A")

    tk.Label(tab_overview, text="Número de Parte:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=0, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=product_id_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=0, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, text="Tiempo desde la última pieza:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=product_read_time_display_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=1, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, text="Contador de Piezas:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=2, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=product_counter_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=2, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, text="Contador de Piezas NOK:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=3, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=product_nok_counter_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=3, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, text="Medición de Altura:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=4, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=product_height_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=4, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, text="Número Serial:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=5, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_overview, textvariable=serial_number_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=5, column=1, sticky="w", pady=2, padx=5)

    tab_plc_readings = tk.LabelFrame(notebook, text="", font=FONT_SECTION_HEADER,
                                     bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                     highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=20, pady=15)
    notebook.add(tab_plc_readings, text="INFO ADICIONAL")
    tab_plc_readings.grid_columnconfigure(0, weight=1)
    tab_plc_readings.grid_columnconfigure(1, weight=3)

    global measurement2_var, measurement3_var, measurement4_var, additional_info1_var, additional_info2_var
    measurement2_var = tk.StringVar(value="Medición 2: N/A")
    measurement3_var = tk.StringVar(value="Medición 3: N/A")
    measurement4_var = tk.StringVar(value="Medición 4: N/A")
    additional_info1_var = tk.StringVar(value="Información Adicional 1: N/A")
    additional_info2_var = tk.StringVar(value="Información Adicional 2: N/A")

    tk.Label(tab_plc_readings, text="Medición 2:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=0, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, textvariable=measurement2_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=0, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, text="Medición 3:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, textvariable=measurement3_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=1, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, text="Medición 4:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=2, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, textvariable=measurement4_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=2, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, text="Información Adicional 1:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=3, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, textvariable=additional_info1_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=3, column=1, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, text="Información Adicional 2:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=4, column=0, sticky="w", pady=2, padx=5)
    tk.Label(tab_plc_readings, textvariable=additional_info2_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_LIGHT).grid(row=4, column=1, sticky="w", pady=2, padx=5)

    # --- Botones Sesión ---
    session_buttons_frame = tk.Frame(main_content_frame, bg=COLOR_BACKGROUND_PRIMARY)
    session_buttons_frame.pack(pady=(10, 20), fill="x", expand=True)
    session_buttons_frame.grid_columnconfigure(0, weight=1)
    session_buttons_frame.grid_columnconfigure(1, weight=1)

    btn_logout = tk.Button(session_buttons_frame, text="Registrar Salida", command=lambda: logout(parent_root),
                           bg=COLOR_ACCENT_BLUE, fg=COLOR_BACKGROUND_PRIMARY, font=FONT_BUTTON, width=18, height=2, cursor="hand2", bd=0, relief="flat",
                           activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    btn_logout.grid(row=0, column=0, padx=10, pady=5, sticky="e")

    btn_exit = tk.Button(session_buttons_frame, text="Salir de la Aplicación", command=on_main_window_close,
                         bg=COLOR_DANGER_RED, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTON, width=18, height=2, cursor="hand2", bd=0, relief="flat",
                         activebackground=COLOR_DANGER_RED, activeforeground=COLOR_TEXT_LIGHT)
    btn_exit.grid(row=0, column=1, padx=10, pady=5, sticky="w")

def logout(parent_root):
    global login_successful_machine, logged_in_user_name_global, logged_in_laser_code_global, logged_in_datetime_global, logged_in_user_number_global
    global product_id_var, product_counter_var, product_read_time_display_var, product_height_var
    global measurement2_var, measurement3_var, measurement4_var, additional_info1_var, additional_info2_var, serial_number_var

    # Escribir Log_Status = 0 en PLC al registrar salida
    logout_machine_user()

    if os.path.exists(LASER_CODE_FILE_PATH):
        try: os.remove(LASER_CODE_FILE_PATH)
        except Exception as e:
            messagebox.showerror("Error de Archivo", f"No se pudo eliminar el archivo de código láser:\n{e}", parent=parent_root)
            write_log("ERROR", f"Error al eliminar archivo de código láser: {e}")

    if logged_in_user_name_global:
        write_log("LOGOUT", f"Usuario '{logged_in_user_name_global}' ({logged_in_laser_code_global}) ha cerrado sesión.")

    login_successful_machine = False
    logged_in_user_name_global = None
    logged_in_laser_code_global = None
    logged_in_user_number_global = None
    logged_in_datetime_global = None

    product_id_var = None
    product_counter_var = None
    product_read_time_display_var = None
    product_height_var = None
    measurement2_var = None
    measurement3_var = None
    measurement4_var = None
    additional_info1_var = None
    additional_info2_var = None
    serial_number_var = None

    show_initial_screen(parent_root)

def show_initial_screen(parent_root):
    global login_successful_machine
    global plc_status_text_var, plc_connection_indicator_canvas, plc_connection_indicator_oval_id, plc_last_update_label_var, plc_error_label_widget, plc_status_label_widget
    global product_id_var, product_counter_var, product_nok_counter_var, product_read_time_display_var, product_height_var, serial_number_var

    login_successful_machine = False

    for w in parent_root.winfo_children():
        w.destroy()

    parent_root.title("Manual Lasercell")
    parent_root.geometry("640x820")
    parent_root.resizable(True, True)
    parent_root.configure(bg=COLOR_BACKGROUND_PRIMARY)

    create_menu_bar(parent_root, enable_download_menu=False, enable_logs_menu=False, enable_serial_menu=False)

    parent_root.update_idletasks()
    x = parent_root.winfo_screenwidth() // 2 - parent_root.winfo_width() // 2
    y = parent_root.winfo_screenheight() // 2 - parent_root.winfo_height() // 2
    parent_root.geometry(f"+{x}+{y}")

    # --- Scrollable main_content_frame ---
    main_canvas = tk.Canvas(parent_root, bg=COLOR_BACKGROUND_PRIMARY, highlightthickness=0)
    main_canvas.pack(side="left", fill="both", expand=True)
    x_scroll = tk.Scrollbar(parent_root, orient="horizontal", command=main_canvas.xview)
    x_scroll.pack(side="bottom", fill="x")
    y_scroll = tk.Scrollbar(parent_root, orient="vertical", command=main_canvas.yview)
    y_scroll.pack(side="right", fill="y")
    main_canvas.configure(xscrollcommand=x_scroll.set, yscrollcommand=y_scroll.set)
    main_content_frame = tk.Frame(main_canvas, bg=COLOR_BACKGROUND_PRIMARY, padx=30, pady=30)
    main_content_frame_id = main_canvas.create_window((0,0), window=main_content_frame, anchor="nw")
    def on_configure(event):
        main_canvas.configure(scrollregion=main_canvas.bbox("all"))
    main_content_frame.bind("<Configure>", on_configure)
    def on_canvas_configure(event):
        main_canvas.itemconfig(main_content_frame_id, width=event.width)
    main_canvas.bind("<Configure>", on_canvas_configure)

    # --- ESTADO DEL PLC ---
    plc_status_frame = tk.LabelFrame(main_content_frame, text="ESTADO DEL PLC", font=FONT_SECTION_HEADER, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE,
                                     bd=1, relief="solid", highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=30, pady=25)
    plc_status_frame.pack(pady=(0, 25), fill="x")
    plc_status_frame.grid_columnconfigure(0, weight=1)
    plc_status_frame.grid_columnconfigure(1, weight=3)

    global plc_connection_indicator_oval_id, plc_connection_indicator_canvas
    plc_connection_indicator_canvas = tk.Canvas(plc_status_frame, width=40, height=40, bg=COLOR_BACKGROUND_SECONDARY, highlightthickness=0)
    plc_connection_indicator_canvas.grid(row=0, column=0, sticky="w", padx=5, pady=2)
    plc_connection_indicator_oval_id = plc_connection_indicator_canvas.create_oval(5, 5, 35, 35, fill=COLOR_PLC_DISCONNECTED, outline=COLOR_PLC_DISCONNECTED)

    global plc_status_text_var, plc_status_label_widget, plc_last_update_label_var, plc_error_label_widget
    plc_status_text_var = tk.StringVar(value="Conectando con PLC...")
    plc_status_label_widget = tk.Label(plc_status_frame, textvariable=plc_status_text_var, font=FONT_STATUS_INFO, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY)
    plc_status_label_widget.grid(row=0, column=1, sticky="w", pady=2, padx=5)

    tk.Label(plc_status_frame, text="Última Actualización:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    plc_last_update_label_var = tk.StringVar(value=datetime.now().strftime("%H:%M:%S"))
    tk.Label(plc_status_frame, textvariable=plc_last_update_label_var, font=FONT_STATUS_INFO, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=1, column=1, sticky="w", pady=2, padx=5)

    plc_error_label_widget = tk.Label(plc_status_frame, text="", font=FONT_STATUS_ERROR, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_DANGER_RED)
    plc_error_label_widget.grid(row=2, column=0, columnspan=2, pady=(10,5), padx=5, sticky="w")
    plc_error_label_widget.grid_remove()

    # --- ÚLTIMO PRODUCTO // NÚMERO DE PARTE ---
    product_data_frame = tk.LabelFrame(main_content_frame, text="ÚLTIMA PIEZA MARCADA", font=FONT_SECTION_HEADER,
                                       bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                       highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=20, pady=15)
    product_data_frame.pack(pady=(0, 25), fill="x")
    product_data_frame.grid_columnconfigure(0, weight=1)
    product_data_frame.grid_columnconfigure(1, weight=3)

    product_id_var = tk.StringVar(value="Número de Parte: N/A")
    product_read_time_display_var = tk.StringVar(value="Tiempo: 00:00")
    product_counter_var = tk.StringVar(value="Piezas: 0")
    product_nok_counter_var = tk.StringVar(value="Piezas NOK: 0")
    product_height_var = tk.StringVar(value="Altura: N/A")
    serial_number_var = tk.StringVar(value="Número Serial: N/A")

    tk.Label(product_data_frame, text="Número de Parte:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=0, column=0, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, textvariable=product_id_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=0, column=1, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, text="Tiempo desde la última pieza:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, textvariable=product_read_time_display_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=1, column=1, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, text="Contador de Piezas:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=2, column=0, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, textvariable=product_counter_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=2, column=1, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, text="Medición de Altura:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=3, column=0, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, textvariable=product_height_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=3, column=1, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, text="Número Serial:", font=FONT_LABEL_BOLD, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=4, column=0, sticky="w", pady=2, padx=5)
    tk.Label(product_data_frame, textvariable=serial_number_var, font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_PRIMARY).grid(row=4, column=1, sticky="w", pady=2, padx=5)

    # --- REGISTRO DE ENTRADA ---
    registro_entrada_frame = tk.LabelFrame(main_content_frame, text="REGISTRO DE ENTRADA", font=FONT_SECTION_HEADER,
                                           bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_ACCENT_BLUE, bd=1, relief="solid",
                                           highlightbackground=COLOR_BORDER_SUBTLE, highlightthickness=1, padx=20, pady=15)
    registro_entrada_frame.pack(pady=(0, 25), fill="x")
    registro_var = tk.StringVar()
    password_var = tk.StringVar()

    registro_entrada_frame.grid_columnconfigure(0, weight=1)
    registro_entrada_frame.grid_columnconfigure(1, weight=2)
    registro_entrada_frame.grid_columnconfigure(2, weight=0, minsize=30)

    tk.Label(registro_entrada_frame, text="Número de Registro:", font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=0, column=0, sticky="w", pady=5, padx=5)
    entry_registro_login = tk.Entry(registro_entrada_frame, textvariable=registro_var, font=FONT_ENTRY, bd=1, relief="solid",
                                    bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_PRIMARY, insertbackground=COLOR_TEXT_PRIMARY,
                                    highlightbackground=COLOR_BORDER_SUBTLE, highlightcolor=COLOR_FOCUS_BORDER, highlightthickness=1)
    entry_registro_login.grid(row=0, column=1, sticky="ew", pady=5, padx=5); entry_registro_login.focus_set()

    tk.Label(registro_entrada_frame, text="Contraseña:", font=FONT_LABEL, bg=COLOR_BACKGROUND_SECONDARY, fg=COLOR_TEXT_MUTED).grid(row=1, column=0, sticky="w", pady=5, padx=5)
    entry_password_login = tk.Entry(registro_entrada_frame, textvariable=password_var, font=FONT_ENTRY, bd=1, relief="solid",
                                    bg=COLOR_BACKGROUND_TERTIARY, fg=COLOR_TEXT_PRIMARY, insertbackground=COLOR_TEXT_PRIMARY,
                                    highlightbackground=COLOR_BORDER_SUBTLE, highlightcolor=COLOR_FOCUS_BORDER, highlightthickness=1, show="*")
    entry_password_login.grid(row=1, column=1, sticky="ew", pady=5, padx=5)

    # Botón invisible para autollenado rápido (dev bypass)
    def dev_autofill(event=None):
        registro_var.set("10195")
        password_var.set("AT195")
        attempt_machine_login(parent_root, registro_var, password_var, entry_registro_login)

    invisible_btn = tk.Frame(registro_entrada_frame, width=20, height=20, bg=COLOR_BACKGROUND_TERTIARY, highlightthickness=0, bd=0)
    invisible_btn.grid(row=1, column=2, sticky="e", padx=5)
    invisible_btn.bind("<Button-3>", dev_autofill)  # Clic derecho

    button_frame_bottom = tk.Frame(main_content_frame, bg=COLOR_BACKGROUND_PRIMARY)
    button_frame_bottom.pack(pady=(10, 20), fill="x", expand=True)
    button_frame_bottom.grid_columnconfigure(0, weight=1)
    button_frame_bottom.grid_columnconfigure(1, weight=1)

    btn_register_entry = tk.Button(button_frame_bottom, text="Registrar Entrada",
                                   command=lambda: attempt_machine_login(parent_root, registro_var, password_var, entry_registro_login),
                                   bg=COLOR_ACCENT_BLUE, fg=COLOR_BACKGROUND_PRIMARY, font=FONT_BUTTON, width=18, height=2, cursor="hand2", bd=0, relief="flat",
                                   activebackground=COLOR_ACCENT_BLUE, activeforeground=COLOR_BACKGROUND_PRIMARY)
    btn_register_entry.grid(row=0, column=0, padx=10, pady=5, sticky="e")

    btn_close_all = tk.Button(button_frame_bottom, text="Salir", command=on_main_window_close,
                              bg=COLOR_DANGER_RED, fg=COLOR_TEXT_PRIMARY, font=FONT_BUTTON, width=18, height=2, cursor="hand2", bd=0, relief="flat",
                              activebackground=COLOR_DANGER_RED, activeforeground=COLOR_TEXT_PRIMARY)
    btn_close_all.grid(row=0, column=1, padx=10, pady=5, sticky="w")

    parent_root.bind('<Return>', lambda event=None: attempt_machine_login(parent_root, registro_var, password_var, entry_registro_login))

def on_main_window_close():
    global logged_in_user_name_global
    stop_monitoring_thread()

    if logged_in_user_name_global:
        write_log("APP_EXIT", f"Aplicación cerrada con usuario '{logged_in_user_name_global}' loggeado.")
    else:
        write_log("APP_EXIT", "Aplicación cerrada (usuario no loggeado).")

    try: _hb_write(0)  # forzar 0 al salir
    except: pass

    if os.path.exists(LASER_CODE_FILE_PATH):
        try: os.remove(LASER_CODE_FILE_PATH)
        except Exception as e: write_log("ERROR", f"Error al eliminar active_laser_code.csv: {e}")

    if root_machine_app and root_machine_app.winfo_exists():
        root_machine_app.destroy()
    sys.exit(0)

# ================================ MAIN =====================================
_mysql_init_schema()  # crea BD si no existe (no truena si MySQL no responde)

if __name__ == "__main__":

    root_machine_app = tk.Tk()
    set_custom_icon(root_machine_app)

    # asegurar directorios
    for d in [
        os.path.dirname(NOMBRE_ARCHIVO_LOGS),
        os.path.dirname(NOMBRE_ARCHIVO_REGISTROS),
        os.path.dirname(LASER_CODE_FILE_PATH),
        PRODUCT_CSV_BASE_PATH,
    ]:
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

    style = ttk.Style()
    style.theme_create("custom_theme", parent="alt", settings={
        "TNotebook": {"configure": {"tabmargins": [2, 5, 2, 0], "background": COLOR_BACKGROUND_SECONDARY}},
        "TNotebook.Tab": {
            "configure": {"padding": [10, 5], "background": COLOR_BACKGROUND_TERTIARY, "foreground": COLOR_TEXT_LIGHT},
            "map": {"background": [("selected", COLOR_BACKGROUND_PRIMARY)],
                    "foreground": [("selected", COLOR_ACCENT_BLUE)],
                    "expand": [("selected", [1,1,1,0])]}
        }
    })
    style.theme_use("custom_theme")

    show_initial_screen(root_machine_app)
    initialize_plc_signals()  # Inicializar Log_Status en False
    start_monitoring_thread()
    root_machine_app.protocol("WM_DELETE_WINDOW", on_main_window_close)
    root_machine_app.mainloop()

    stop_monitoring_thread()
    sys.exit(0)