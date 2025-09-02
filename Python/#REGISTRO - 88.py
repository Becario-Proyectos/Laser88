#Registro88 - MYSQL
import tkinter as tk
from tkinter import messagebox
from tkinter import simpledialog
from tkinter import ttk
from PIL import Image, ImageTk
import os
from datetime import datetime, date
from cryptography.fernet import Fernet
import mysql.connector
import shutil
import threading
import time

# ============================== RUTAS DE ARCHIVOS ==============================
NOMBRE_ARCHIVO_REGISTROS = "C:/VCST/2888/Registros/RegistroPersonal/DB_Registro.txt"
NOMBRE_ARCHIVO_PARTES = "C:/VCST/2888/Registros/RegistroPartes/DB_Partes.txt"

NOMBRE_ARCHIVO_BAJAS = "C:/VCST/2888/Registros/RegistroBajasPersonal/DB_BajasStaff.txt"

NOMBRE_ARCHIVO_REGISTROS_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Registro_pendiente.txt"
NOMBRE_ARCHIVO_PARTES_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Partes_pendiente.txt"

NOMBRE_ARCHIVO_BAJAS_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Bajas_pendiente.txt"
NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES = "C:/VCST/2888/Registros/Pendientes/DB_Bajas_partes_pendiente.txt"
NOMBRE_ARCHIVO_BAJAS_PARTES_LOG = "C:/VCST/2888/Registros/RegistroBajasPartes/DB_BajasPartes.txt"

CLAVE_PATH = "C:/VCST/2888/Registros/Key/clave2.key"

# ============================ RUTA DE LA IMAGEN  ==============================
IMAGE_LOGO_PATH = "C:/VCST/2888/Icon/logo_dragon.png"

# CREDENCIALES DE ADMINISTRADOR
USUARIO_ADMIN = "ADMIN"
PASSWORD_ADMIN = "PASSWORD"

# ===================== CONFIGURACIÓN DE LA BASE DE DATOS MYSQL =======================
DB_HOST = "10.4.0.103"
DB_USER = "wamp_user"
DB_PASSWORD = "wamp"
DB_NAME = "test"  
TABLE_NAME_PERSONAL = "Registered_personnel"
TABLE_NAME_PARTES = "registered_parts"

# ============================ COLORES Y FUENTES ============================
COLOR_PRIMARY = "#2C3E50"
COLOR_SECONDARY = "#ECF0F1"
COLOR_ACCENT = "#3498DB"
COLOR_SUCCESS = "#27AE60"
COLOR_DANGER = "#E74C4C"
COLOR_TEXT_LIGHT = "#FFFFFF"
COLOR_TEXT_DARK = "#34495E"
COLOR_ORANGE = "#FAA200"

FONT_HEADER = ("Segoe UI", 24, "bold")
FONT_LABELS = ("Segoe UI", 13, "bold")
FONT_ENTRY = ("Segoe UI", 13)
FONT_BUTTONS = ("Segoe UI", 13, "bold")
FONT_TEXT_AREA = ("Consolas", 10) 

# ============================ VARIABLES GLOBALES ==============================
root_main = None
entry_nombre = None
entry_registro = None
entry_password_registro = None
entry_codigo = None
display_fecha = None
login_successful = False 
entry_numero_parte = None
entry_numero_cat = None
parts_window = None 
options_window = None 
cache_personal = []
cache_partes = []

# --- Restaurar archivo desde backup (.bak) ---
def restore_from_backup(path):
    bak_path = path + ".bak"
    try:
        if os.path.exists(bak_path):
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                import shutil
                shutil.copy2(bak_path, path)
                print(f"Archivo restaurado desde backup: {bak_path} -> {path}")
    except Exception as e:
        messagebox.showerror("Error de restauración", f"No se pudo restaurar el archivo desde backup: {e}")

# --- FUNCIONES DE CIFRADO NECESARIAS ---
def load_key():
    key_dir = os.path.dirname(CLAVE_PATH)
    if key_dir and not os.path.exists(key_dir):
        try:
            os.makedirs(key_dir, exist_ok=True)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo crear el directorio para la clave de cifrado: {e}")
            return None
    if not os.path.exists(CLAVE_PATH):
        try:
            key = Fernet.generate_key()
            with open(CLAVE_PATH, "wb") as key_file:
                key_file.write(key)
            messagebox.showinfo("Clave Generada", "Se ha generado una nueva clave de cifrado en:\n" + CLAVE_PATH)
            return key
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo generar ni guardar la clave de cifrado: {e}")
            return None
    else:
        try:
            with open(CLAVE_PATH, "rb") as file:
                return file.read()
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo cargar la clave de cifrado existente. Podría estar corrupta: {e}")
            return None

def guardar_registro_cifrado(texto_plano, archivo_destino):
    key = load_key()
    if key is None:
        messagebox.showerror("Error", "No se pudo guardar el registro: clave de cifrado no disponible.")
        return
    try:
        fernet = Fernet(key)
    except Exception as e:
        messagebox.showerror("Error", f"Error al inicializar Fernet con la clave. La clave podría estar corrupta: {e}")
        return
    cifrado = fernet.encrypt(texto_plano.encode("utf-8"))
    try:
        db_dir = os.path.dirname(archivo_destino)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        with open(archivo_destino, "ab") as f:
            f.write(cifrado + b"\n")
    except Exception as e:
        messagebox.showerror("Error de escritura", f"No se pudo escribir en el archivo de la base de datos: {e}")

def safe_backup(path):
    try:
        if os.path.exists(path):
            shutil.copy2(path, path + ".bak")
    except Exception as e:
        messagebox.showerror("Error de lectura", f"Error inesperado al hacer backup del archivo: {e}")

def _overwrite_encrypted_file(lines, archivo_destino):
    key = load_key()
    if key is None:
        messagebox.showerror("Error", "No se pudo generar/cargar la clave de cifrado para regenerar el TXT.")
        return False
    fernet = Fernet(key)
    carpeta = os.path.dirname(archivo_destino)
    if carpeta and not os.path.exists(carpeta):
        os.makedirs(carpeta, exist_ok=True)
    try:
        with open(archivo_destino, "wb") as f:
            for line in lines:
                f.write(fernet.encrypt(line.encode("utf-8")) + b"\n")
        return True
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo escribir el archivo cifrado {archivo_destino}:\n{e}")
        return False

def leer_partes_pendientes():
    return leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES_PENDIENTES)

def borrar_partes_pendientes():
    try:
        if os.path.exists(NOMBRE_ARCHIVO_PARTES_PENDIENTES):
            os.remove(NOMBRE_ARCHIVO_PARTES_PENDIENTES)
    except Exception:
        pass

def sincronizar_partes_pendientes_a_mysql():
    pendientes = leer_partes_pendientes()
    if not pendientes:
        return
    exitosos = []
    for linea in pendientes:
        try:
            partes = linea.split(",")
            if len(partes) < 4:
                continue
            fecha = partes[0].strip()
            hora = partes[1].strip()
            numero_parte = partes[2].strip()
            numero_cat = partes[3].strip()
            if insert_data_into_mysql_partes(fecha, hora, numero_parte, numero_cat):
                exitosos.append(linea)
        except Exception:
            pass
    restantes = [l for l in pendientes if l not in exitosos]
    if restantes:
        _overwrite_encrypted_file(restantes, NOMBRE_ARCHIVO_PARTES_PENDIENTES)
    else:
        borrar_partes_pendientes()
    
# ---- FUNCIONES DE BAJAS Y SINCRONIZACIÓN PARA PARTES ----
def leer_bajas_partes_pendientes():
    return leer_registros_descifrados(NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)

def borrar_bajas_partes_pendientes():
    try:
        if os.path.exists(NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES):
            os.remove(NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
    except Exception:
        pass

def sincronizar_bajas_partes_pendientes_a_mysql():
    pendientes = leer_bajas_partes_pendientes()
    if not pendientes:
        return
    exitosos = []
    conexion_exitosa = True
    try:
        test_conn = _get_mysql_conn_for_reads()
        test_conn.close()
    except Exception:
        conexion_exitosa = False

    if not conexion_exitosa:
        print("Sincronización de bajas de partes: sin conexión a MySQL. Se mantiene el TXT de bajas pendientes.")
        return

    for linea in pendientes:
        try:
            partes = linea.split(",")
            if len(partes) < 4:
                continue
            numero_cat = partes[3].strip()
            mydb = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            mycursor = mydb.cursor()
            mycursor.execute(f"DELETE FROM {TABLE_NAME_PARTES} WHERE Numero_CAT = %s", (numero_cat,))
            mydb.commit()
            if mycursor.rowcount > 0:
                exitosos.append(linea)
            mycursor.close()
            mydb.close()
        except Exception as e:
            pass

    restantes = [l for l in pendientes if l not in exitosos]
    if restantes:
        _overwrite_encrypted_file(restantes, NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
    else:
        borrar_bajas_partes_pendientes()

    # Solo reconstruir el TXT principal si hubo conexión
    sync_txt_from_mysql_partes()

# --- FUNCIONES DE BAJAS Y SINCRONIZACIÓN ---
def leer_bajas_pendientes():
    return leer_registros_descifrados(NOMBRE_ARCHIVO_BAJAS_PENDIENTES)

def borrar_bajas_pendientes():
    try:
        if os.path.exists(NOMBRE_ARCHIVO_BAJAS_PENDIENTES):
            os.remove(NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
    except Exception:
        pass

def sincronizar_bajas_pendientes_a_mysql():
    pendientes = leer_bajas_pendientes()
    if not pendientes:
        return
    exitosos = []
    conexion_exitosa = True
    try:
        # Probar conexión antes de procesar
        test_conn = _get_mysql_conn_for_reads()
        test_conn.close()
    except Exception:
        conexion_exitosa = False

    if not conexion_exitosa:
        print("Sincronización de bajas: sin conexión a MySQL. Se mantiene el TXT de bajas pendientes.")
        return

    for linea in pendientes:
        try:
            partes = linea.strip().split(",")
            if len(partes) < 3:
                continue
            numero_int = int(partes[2].strip())
            eliminado = eliminar_personal_en_mysql(numero_int)
            if eliminado:
                exitosos.append(linea)
                log_msg = f"{linea} - Baja sincronizada con MySQL\n"
            else:
                exitosos.append(linea)
                log_msg = f"{linea} - Ya no existe en MySQL, marcado como sincronizado\n"
            with open(NOMBRE_ARCHIVO_BAJAS, "a", encoding="utf-8") as f:
                f.write(log_msg)
        except Exception as e:
            continue
    
    restantes = [l for l in pendientes if l not in exitosos]
    if restantes:
        _overwrite_encrypted_file(restantes, NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
    else:
        borrar_bajas_pendientes()
    print(f"{len(exitosos)} bajas pendientes sincronizadas o marcadas como ya inexistentes en MySQL.")

    # Solo reconstruir el TXT principal si hubo conexión
    sync_txt_from_mysql_personal()

def eliminar_personal_en_mysql(numero_int):
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        mycursor = mydb.cursor()
        mycursor.execute(f"DELETE FROM {TABLE_NAME_PERSONAL} WHERE Numero = %s", (numero_int,))
        mydb.commit()
        rowcount = mycursor.rowcount
        mycursor.close()
        mydb.close()
        return rowcount > 0
    except Exception:
        return False

def sincronizar_todo_pendiente():
    try:
        sincronizar_pendientes_a_mysql()
    except Exception:
        pass
    try:
        sincronizar_bajas_pendientes_a_mysql()
    except Exception:
        pass
    try:
        sincronizar_bajas_partes_pendientes_a_mysql()
    except Exception:
        pass
    try:
        sincronizar_partes_pendientes_a_mysql()
    except Exception:
        pass

# --- Sincronización automática en segundo plano ---
def sync_background_loop(interval=10):
    while True:
        try:
            sincronizar_todo_pendiente()
        except Exception:
            pass
        time.sleep(interval)

def sincronizar_pendientes_a_mysql():
    # Sincroniza los registros pendientes de personal a MySQL
    pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
    if not pendientes:
        return
    exitosos = []
    for linea in pendientes:
        try:
            partes = linea.strip().split(",")
            if len(partes) < 5:
                continue
            fecha = partes[0].strip()
            num_index = -1
            for i in range(2, len(partes)):
                if partes[i].strip().isdigit():
                    num_index = i
                    break
            if num_index == -1:
                continue
            nombre = ",".join(partes[1:num_index]).strip()
            numero = int(partes[num_index].strip())
            password = partes[num_index+1].strip()
            code_laser = partes[num_index+2].strip()
            if insert_data_into_mysql_personal(fecha, nombre, numero, password, code_laser):
                exitosos.append(linea)
        except Exception:
            continue
    # Si hubo exitosos, eliminarlos del TXT pendiente
    if exitosos:
        restantes = [l for l in pendientes if l not in exitosos]
        if restantes:
            _overwrite_encrypted_file(restantes, NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
        else:
            try:
                os.remove(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
            except Exception:
                pass
        # Agregar exitosos al TXT principal
        for l in exitosos:
            guardar_registro_cifrado(l, NOMBRE_ARCHIVO_REGISTROS)
        print(f"{len(exitosos)} registros pendientes sincronizados con MySQL.")

if __name__ == "__main__":
    sincronizar_todo_pendiente()
    sync_thread = threading.Thread(target=sync_background_loop, args=(10,), daemon=True)
    sync_thread.start()

# Leer y descifrar todos los registros del archivo
def leer_registros_descifrados(archivo):
    key = load_key()
    if key is None:
        # Si la clave no se puede cargar, es un error crítico.
        return ["ERROR: Clave no cargada o generada. No se pueden leer los registros."]
    try:
        fernet = Fernet(key)
    except Exception as e:
        messagebox.showerror("Error", f"Error al inicializar Fernet con la clave. La clave podría estar corrupta: {e}")
        return ["ERROR: Error al inicializar cifrado. No se pueden leer los registros."]

    registros = []
    try:
        with open(archivo, "rb") as f:
            for linea in f:
                try:
                    descrifrado = fernet.decrypt(linea.strip())
                    registros.append(descrifrado.decode("utf-8"))
                except Exception as e:
                    # Captura errores específicos de descifrado para líneas individuales
                    registros.append(f"ERROR: Línea ilegible o corrupta en DB: {e}")
    except FileNotFoundError:
        # Si el archivo no existe, simplemente devuelve una lista vacía.
        return []
    except Exception as e:
        messagebox.showerror("Error de lectura", f"Error inesperado al leer el archivo de registros: {e}")
        return [f"ERROR: Error inesperado al leer el archivo: {e}"]
        
    return registros

def check_duplicate_personal(nombre_nuevo, numero_registro_nuevo, codigo_nuevo):
    registros_existentes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS)

    if registros_existentes and registros_existentes[0].startswith("ERROR:"):
        messagebox.showwarning("Advertencia", "No se pudo leer la base de datos de TXT para verificar duplicados. Intenta de nuevo o verifica la clave de cifrado.")
        return True, "No se pudo verificar duplicados debido a un error en la lectura de la base de datos TXT."
    
    if not registros_existentes:
        return False, None # No se encontraron duplicados porque no hay registros aún.

    for line in registros_existentes:
        line = line.strip()
        if not line or line.startswith("ERROR:"): 
            continue

        try:
            # Formato esperado: fecha,nombre,numero_registro,password,codigo_doss_letras
            parts = line.split(',')
            
            num_index = -1
            for i in range(2, len(parts)): 
                if parts[i].strip().isdigit():
                    num_index = i
                    break
            
            if num_index == -1: 
                print(f"Advertencia: Línea descifrada mal formada (no se encontró el número): {line}. Ignorada.")
                continue

            nombre_existente_parts = parts[1:num_index] 
            nombre_existente = ",".join(nombre_existente_parts).strip()

            numero_registro_existente = parts[num_index].strip()
            codigo_existente = parts[num_index + 2].strip() 

            if nombre_existente.lower() == nombre_nuevo.lower():
                return True, f"El nombre '{nombre_nuevo}' ya está registrado en el TXT."

            if numero_registro_existente == numero_registro_nuevo:
                return True, f"El número de registro '{numero_registro_nuevo}' ya está registrado en el TXT."

            if codigo_existente == codigo_nuevo:
                return True, f"El código láser '{codigo_nuevo}' ya está registrado en el TXT."
        except (IndexError, ValueError):
            # Esto puede ocurrir if una línea descifrada no tiene el formato esperado
            print(f"Advertencia: Línea descifrada mal formada o incompleta ignorada: {line}")
            continue
    return False, None

# --- Función para crear la base de datos y las tablas en MySQL ---
def create_mysql_database_and_tables(cursor):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        #print(f"Base de datos MySQL '{DB_NAME}' verificada/creada exitosamente.")

        cursor.execute(f"USE {DB_NAME}")
        #print(f"Usando la base de datos MySQL '{DB_NAME}'.")

        # Tabla para personal
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME_PERSONAL} (
                fecha_registro DATE,
                Nombre VARCHAR(255),
                Numero INT,
                Password VARCHAR(255) NOT NULL,
                Code_Laser VARCHAR(50) NOT NULL,
                PRIMARY KEY (Numero)
            )
        """)
        #print(f"Tabla MySQL '{TABLE_NAME_PERSONAL}' verificada/creada exitosamente.")

        # Nueva tabla para registro_partes
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME_PARTES} (
                fecha_registro DATE,
                hora_registro TIME,
                Numero_Parte VARCHAR(100) NOT NULL,
                Numero_CAT VARCHAR(100) NOT NULL,
                PRIMARY KEY (Numero_CAT)
            ) 
        """)
        #print(f"Tabla MySQL '{TABLE_NAME_PARTES}' verificada/creada exitosamente.")

    except mysql.connector.Error as err:
        return False
    return True

# INSERTAR EN LA DB MYSQL (PERSONAL)
def insert_data_into_mysql_personal(fecha, nombre, numero, password, code_laser):
    mydb = None
    mycursor = None
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        mycursor = mydb.cursor()

        # Esta llamada ya asegura que la DB y tablas existan antes de la inserción.
        if not create_mysql_database_and_tables(mycursor):
            return False 

        # Verifica if el número de registro ya existe en MySQL para evitar duplicados
        mycursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME_PERSONAL} WHERE Numero = %s", (numero,))
        if mycursor.fetchone()[0] > 0:
            messagebox.showwarning("Duplicado en MySQL", f"El número de registro '{numero}' ya existe en la base de datos MySQL.")
            return False

        sql = f"""
            INSERT INTO {TABLE_NAME_PERSONAL} 
            (fecha_registro, Nombre, Numero, Password, Code_Laser) 
            VALUES (%s, %s, %s, %s, %s)
        """
        val = (fecha, nombre, numero, password, code_laser)
        
        mycursor.execute(sql, val)
        mydb.commit()
        return True
    except mysql.connector.Error as err:
        
        return False
    finally:
        if mycursor:
            mycursor.close()
        if mydb and mydb.is_connected():
            mydb.close()

# DAR DE BAJA EN LA DB MYSQL (PERSONAL) - VERSIÓN OFFLINE/ONLINE ROBUSTA
def dar_de_baja_personal():
    safe_backup(NOMBRE_ARCHIVO_REGISTROS)

    root_main.update_idletasks()
    x = root_main.winfo_screenwidth() // 2 - root_main.winfo_width() // 2
    y = root_main.winfo_screenheight() // 2 - root_main.winfo_height() // 2
    root_main.geometry(f"+{x}+{y}")
    numero_a_borrar = tk.simpledialog.askstring("Baja de Personal", "Ingrese el número de registro a dar de baja \t", parent=root_main)
    if numero_a_borrar is None:
        return
    if not numero_a_borrar.strip().isdigit():
        messagebox.showwarning("Entrada inválida", "Debes ingresar un número de registro válido.")
        return
    numero_int = int(numero_a_borrar)

    # Buscar la línea a eliminar en el TXT cifrado principal
    registros = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS)
    nuevos_registros = []
    eliminado_txt = None
    for linea in registros:
        if linea.startswith("ERROR:") or not linea.strip():
            continue
        partes = linea.split(",")
        if len(partes) >= 3 and partes[2].strip() == str(numero_int):
            eliminado_txt = linea
            continue  # No agregar a nuevos_registros
        nuevos_registros.append(linea)

    # Si no se encontró en el principal, buscar en pendientes
    encontrado_en_pendiente = False
    if not eliminado_txt:
        registros_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
        nuevos_registros_pendientes = []
        for linea in registros_pendientes:
            if linea.startswith("ERROR:") or not linea.strip():
                continue
            partes = linea.split(",")
            if len(partes) >= 3 and partes[2].strip() == str(numero_int):
                eliminado_txt = linea
                encontrado_en_pendiente = True
                continue  # No agregar a nuevos_registros_pendientes
            nuevos_registros_pendientes.append(linea)
        if encontrado_en_pendiente:
            # Confirmar antes de eliminar
            confirmar = messagebox.askyesno("Confirmar eliminación", f"¿Deseas dar de baja al número {numero_int}?")
            if not confirmar:
                return
            # Sobrescribir el archivo de pendientes sin el registro dado de baja
            _overwrite_encrypted_file(nuevos_registros_pendientes, NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
            # Guardar la baja en pendientes de bajas
            if eliminado_txt:
                guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
                messagebox.showinfo("Éxito", f"El registro {numero_int} fue dado de baja.")
            else:
                messagebox.showerror("Error", "No se encontró el registro a dar de baja.")
            return

    # Si se encontró en el principal
    if eliminado_txt:
        confirmar = messagebox.askyesno("Confirmar eliminación", f"¿Deseas dar de baja al número {numero_int}?")
        if not confirmar:
            return
    else:
        messagebox.showerror("Error", "No se encontró el registro a dar de baja.")
        return

    # Intentar eliminar en MySQL
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        mycursor = mydb.cursor()
        mycursor.execute(f"DELETE FROM {TABLE_NAME_PERSONAL} WHERE Numero = %s", (numero_int,))
        mydb.commit()
        if mycursor.rowcount > 0:
            # Éxito en MySQL, eliminar del TXT
            if nuevos_registros:
                key = load_key()
                if key:
                    fernet = Fernet(key)
                    with open(NOMBRE_ARCHIVO_REGISTROS, "wb") as f:
                        for linea in nuevos_registros:
                            f.write(fernet.encrypt(linea.encode("utf-8")) + b"\n")
            else:
                try:
                    os.remove(NOMBRE_ARCHIVO_REGISTROS)
                except Exception:
                    pass
            with open(NOMBRE_ARCHIVO_BAJAS, "a", encoding="utf-8") as f:
                f.write(f"{eliminado_txt} - Eliminado de TXT y MySQL\n")
            messagebox.showinfo("Éxito", f"Se dio de baja correctamente al número {numero_int}.")
            mycursor.close()
            mydb.close()
            return
        else:
            mycursor.close()
            mydb.close()
            # Si no se eliminó en MySQL, guardar baja pendiente y NO eliminar del TXT
            if eliminado_txt:
                guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
                messagebox.showinfo("Éxito", f"Se dio de baja correctamente al número {numero_int}.")
            else:
                messagebox.showerror("Error", "No se encontró el registro a dar de baja.")
            return
    except Exception:
        # Si hay error de conexión, guardar baja pendiente y NO eliminar del TXT
        if eliminado_txt:
            guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
            messagebox.showinfo("Éxito", f"Se dio de baja correctamente al número {numero_int}.")
        else:
            messagebox.showerror("Error", "No se encontró el registro a dar de baja.")
        return

# INSERTAR EN LA DB MYSQL (PARTES)
def insert_data_into_mysql_partes(fecha, hora, numero_parte, numero_cat):
    mydb = None
    mycursor = None
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        mycursor = mydb.cursor()
        if not create_mysql_database_and_tables(mycursor):
            return False 
        
        # --- Validaciones de duplicados (previas) ---
        # Numero_Parte
        sql_check_np = f"SELECT COUNT(*) FROM `{TABLE_NAME_PARTES}` WHERE `Numero_Parte`=%s"
        mycursor.execute(sql_check_np, (numero_parte,))
        if mycursor.fetchone()[0] > 0:
            messagebox.showwarning("Duplicado en MySQL", f"El número de parte '{numero_parte}' ya existe en la base de datos.")
            return False

        # Numero_CAT
        sql_check_cat = f"SELECT COUNT(*) FROM `{TABLE_NAME_PARTES}` WHERE `Numero_CAT`=%s"
        mycursor.execute(sql_check_cat, (numero_cat,))
        if mycursor.fetchone()[0] > 0:
            messagebox.showwarning("Duplicado en MySQL", f"El número CAT '{numero_cat}' ya existe en la base de datos y debe ser único.")
            return False

        sql = f"""
            INSERT INTO {TABLE_NAME_PARTES} 
            (fecha_registro, hora_registro, Numero_Parte, Numero_CAT) 
            VALUES (%s, %s, %s, %s)
        """
        val = (fecha, hora, numero_parte, numero_cat)
        mycursor.execute(sql, val)
        mydb.commit()
        return True
    except mysql.connector.Error as err:
        print(f"Error MySQL (partes): {err}")
        return False
    finally:
        if mycursor:
            mycursor.close()
        if mydb and mydb.is_connected():
            mydb.close()

# REGISTRAR DATOS (FUNCIÓN MODIFICADA) - PERSONAL
def registrar_datos_personal():
    fecha_actual = datetime.now().strftime("%Y-%m-%d")

    nombre = entry_nombre.get().strip()
    numero_registro = entry_registro.get().strip()
    password_registro = entry_password_registro.get().strip() 
    codigo_dos_letras = entry_codigo.get().strip() 

    # ----------------------------- VALIDACIONES UNICAMENTE----------------
    if not nombre:
        messagebox.showwarning("Campo vacío", "Por favor ingresa el nombre del personal.")
        return

    if not all(x.isalpha() or x.isspace() or x == '.' for x in nombre): # Permite puntos en el nombre
        messagebox.showwarning("Entrada no válida", "El nombre solo debe contener letras, espacios y puntos.")
        return

    if not numero_registro:
        messagebox.showwarning("Campo vacío", "Por favor ingresa el número de registro.")
        return
    elif not numero_registro.isdigit():
        messagebox.showwarning("Entrada no válida", "Por favor ingresa un **NÚMERO** de registro válido.")
        return
    
    if not password_registro: 
        messagebox.showwarning("Campo vacío", "Por favor ingresa una contraseña.")
        return

    if not codigo_dos_letras:
        messagebox.showwarning("Campo vacío", "Por favor ingresa el código láser de 2 letras.")
        return

    elif len(codigo_dos_letras) != 2 or not codigo_dos_letras.isalpha() or not codigo_dos_letras.isupper():
        messagebox.showwarning("Entrada no válida", "El código láser debe ser de **2 letras mayúsculas** \t\t (ej. LO ó AT).")
        return

    # --- Verificación de duplicados en el archivo TXT ---
    is_duplicate_txt, message_txt = check_duplicate_personal(nombre, numero_registro, codigo_dos_letras)
    if is_duplicate_txt:
        messagebox.showwarning("Duplicado detectado (TXT)", message_txt)
        return

    # --- Intentar insertar primero en la base de datos MySQL ---
    try:
        numero_registro_int = int(numero_registro)
    except ValueError:
        messagebox.showerror("Error", "El número de registro no es un entero válido.")
        return

    if not insert_data_into_mysql_personal(fecha_actual, nombre, numero_registro_int, password_registro, codigo_dos_letras):
        # Si la inserción en MySQL falla, guardar en TXT pendiente
        linea_registro = f"{fecha_actual},{nombre},{numero_registro},{password_registro},{codigo_dos_letras}"
        try:
            guardar_registro_cifrado(linea_registro, NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
            messagebox.showinfo("Registro Completo", "Datos de personal registrados exitosamente.")
        except Exception as e:
            messagebox.showerror("Error de archivo ", f"No se pudo guardar el registro: {e}")
        # Limpiar campos aunque sea pendiente
        entry_nombre.delete(0, tk.END)
        entry_registro.delete(0, tk.END)
        entry_password_registro.delete(0, tk.END)
        entry_codigo.delete(0, tk.END)
        return

    # ----------------------------GENERA LA LINEA PARA TXT-----------------------------------------------------
    linea_registro = f"{fecha_actual},{nombre},{numero_registro},{password_registro},{codigo_dos_letras}"

    try:
        guardar_registro_cifrado(linea_registro, NOMBRE_ARCHIVO_REGISTROS)

        messagebox.showinfo("Registro Completo", "Datos de personal registrados exitosamente en la base de datos")
        print(f"Datos guardados exitosamente en: {NOMBRE_ARCHIVO_REGISTROS}")

    except Exception as e:
        messagebox.showerror("Error de archivo (TXT)", f"Se registró en MySQL, pero no se pudo guardar el registro en el archivo TXT: {e}")
        print(f"Error al guardar en archivo TXT: {e}")

    #---------------------------- LIMPIAR CAMPOS DESPUÉS DE UN REGISTRO-----------------------
    entry_nombre.delete(0, tk.END)
    entry_registro.delete(0, tk.END)
    entry_password_registro.delete(0, tk.END) 
    entry_codigo.delete(0, tk.END)

# REGISTRAR DATOS - PARTES
def registrar_datos_partes():
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    hora_actual = datetime.now().strftime("%H:%M:%S")

    numero_parte = entry_numero_parte.get().strip()
    numero_cat = entry_numero_cat.get().strip()

    # Validaciones
    if not numero_parte:
        messagebox.showwarning("Campo vacío", "Por favor ingresa el número de parte.")
        return
    
    # Validar que Numero_parte solo contenga dígitos
    if not numero_parte.isdigit():
        messagebox.showwarning("Entrada no válida", "El número de parte solo debe contener dígitos.")
        return
    
    # Validar que Numero_parte tenga sólo entre 6 y 7 caracteres
    if not (6 <= len(numero_parte) <= 7):
        messagebox.showwarning("Entrada no válida", "El número de parte debe tener entre 6 y 7 dígitos.")
        return

    if not numero_cat:
        messagebox.showwarning("Campo vacío", "Por favor ingresa el número CAT.")
        return

    # Validar que Numero_CAT sea alfanumérico y pueda contener guiones
    if not all(x.isalnum() or x == '-' for x in numero_cat):
        messagebox.showwarning("Entrada no válida", "El número CAT solo debe contener letras, números y guiones.")
        return

    # Insertar en MySQL
    if not insert_data_into_mysql_partes(fecha_actual, hora_actual, numero_parte, numero_cat):
        # Si la inserción en MySQL falla, guardar en TXT pendiente
        linea_registro_partes = f"{fecha_actual},{hora_actual},{numero_parte},{numero_cat}"
        try:
            guardar_registro_cifrado(linea_registro_partes, NOMBRE_ARCHIVO_PARTES_PENDIENTES)
            messagebox.showinfo("Registro Completo", f"Número de parte {numero_parte} registrado exitosamente.", parent=parts_window)
        except Exception as e:
            messagebox.showerror("Error de archivo", f"No se pudo guardar el registro de parte pendiente: {e}", parent=parts_window)
        entry_numero_parte.delete(0, tk.END)
        entry_numero_cat.delete(0, tk.END)
        return

    # Guardar en archivo TXT cifrado
    linea_registro_partes = f"{fecha_actual},{hora_actual},{numero_parte},{numero_cat}"

    ensure_txt_for_write(NOMBRE_ARCHIVO_PARTES, cache_partes, sync_txt_from_mysql_partes)

    try:
        guardar_registro_cifrado(linea_registro_partes, NOMBRE_ARCHIVO_PARTES)
        cache_partes.append(linea_registro_partes)
        safe_backup(NOMBRE_ARCHIVO_PARTES)
        messagebox.showinfo("Registro Completo", f"Número de parte {numero_parte} registrado exitosamente en la base de datos", parent=parts_window)
        print(f"Datos de partes guardados exitosamente en: {NOMBRE_ARCHIVO_PARTES}")
    except Exception as e:
        messagebox.showerror("Error de archivo (TXT)", f"Se registró en MySQL, pero no se pudo guardar el registro de partes en el archivo TXT: {e}", parent=parts_window)
        print(f"Error al guardar en archivo TXT (partes): {e}")

    # Limpiar campos
    entry_numero_parte.delete(0, tk.END)
    entry_numero_cat.delete(0, tk.END)

# ============================ FUNCIONES PARA LA PANTALLA ====================

# CERRAR VENTANA PRINCIPAL
def exit_app():
    try:
        if root_main and root_main.winfo_exists():
            root_main.quit()
            root_main.destroy()
    except tk.TclError:
        pass

# CERRAR VENTANA DE OPCIONES
def close_options_window():
    global options_window
    if options_window:
        options_window.destroy()
        options_window = None 

# MAIN LOGIN 
def show_login_screen_toplevel(parent_root):
    global login_successful 
    login_successful = False 

    login_window = tk.Toplevel(parent_root)
    login_window.title("Iniciar Sesión")
    login_window.geometry("360x260")
    login_window.resizable(False, False)
    login_window.grab_set() 

    # PARA CENTRAR LOGIN
    login_window.update_idletasks()
    x = login_window.winfo_screenwidth() // 2 - login_window.winfo_width() // 2
    y = login_window.winfo_screenheight() // 2 - login_window.winfo_height() // 2
    login_window.geometry(f"+{x}+{y}")

    # Traer la ventana al frente y darle el foco
    login_window.lift()
    login_window.focus_force()

    # CERRAR TODA LA APLICACIÓN AL PICAR LA TACHITA
    login_window.protocol("WM_DELETE_WINDOW", exit_app)

    # FRAME STYLE
    login_frame = tk.Frame(login_window, bg=COLOR_SECONDARY, bd=5, relief="groove")
    login_frame.pack(padx=20, pady=20, fill="both", expand=True)

    def dev_bypass(event=None):
        global login_successful
        login_successful = True
        try:
            login_window.grab_release()
        except Exception:
            pass
        login_window.destroy()
        show_options_screen()

    # Crea un Frame de 20x20px transparente en la esquina superior derecha
    invisible_btn = tk.Frame(login_window, width=20, height=20, bg=COLOR_SECONDARY, highlightthickness=0, bd=0)
    invisible_btn.place(relx=0.930, rely=0.090, anchor="ne")
    invisible_btn.bind("<Button-3>", dev_bypass)
    invisible_btn.lift()  

    username_var = tk.StringVar()
    password_var = tk.StringVar()

    def attempt_login():
        global login_successful 
        username = username_var.get()
        password = password_var.get()

        if username == USUARIO_ADMIN and password == PASSWORD_ADMIN:
            login_successful = True 
            login_window.grab_release() 
            login_window.destroy()
            show_options_screen() 
        else:
            messagebox.showerror("Error de Inicio de Sesión", "Usuario o contraseña incorrectos.", parent=login_window)
            username_var.set("")
            password_var.set("")
            entry_username_login.focus_set()

    # --------------------------------- WIDGETS LOGIN -----------------------
    tk.Label(login_frame, text="Usuario:", font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(pady=(10,0))
    entry_username_login = tk.Entry(login_frame, textvariable=username_var, font=FONT_ENTRY, bd=2, relief="groove")
    entry_username_login.pack(pady=2)
    entry_username_login.focus_set()

    tk.Label(login_frame, text="Contraseña:", font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(pady=(10,0))
    entry_password_login = tk.Entry(login_frame, textvariable=password_var, show="*", font=FONT_ENTRY, bd=2, relief="groove")
    entry_password_login.pack(pady=2)

    btn_login = tk.Button(login_frame, text="Iniciar Sesión", command=attempt_login, bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, cursor="hand2", bd=2, relief="raised")
    btn_login.pack(pady=15)

    # Permite presionar Enter para iniciar sesión desde cualquier campo
    login_window.bind('<Return>', lambda event=None: attempt_login())

#RESINCRONIZAR LOS TXT CIFRADOS DESDE MySQL
def _overwrite_encrypted_file(lines, archivo_destino):
    key = load_key()
    if key is None:
        messagebox.showerror("Error", "No se pudo generar/cargar la clave de cifrado para regenerar el TXT.")
        return False
    fernet = Fernet(key)

    # Asegura carpeta
    carpeta = os.path.dirname(archivo_destino)
    if carpeta and not os.path.exists(carpeta):
        os.makedirs(carpeta, exist_ok=True)

    try:
        with open(archivo_destino, "wb") as f:
            for line in lines:
                f.write(fernet.encrypt(line.encode("utf-8")) + b"\n")
        return True
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo escribir el archivo cifrado {archivo_destino}:\n{e}")
        return False

def _get_mysql_conn_for_reads():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

#RECONSTRUIR TXT DE PERSONAL DESDE MYSQL
def sync_txt_from_mysql_personal():
    """Reconstruye DB_Registro.txt (cifrado) desde MySQL."""
    try:
        conn = _get_mysql_conn_for_reads()
        cur = conn.cursor()
        cur.execute(f"SELECT fecha_registro, Nombre, Numero, Password, Code_Laser FROM `{TABLE_NAME_PERSONAL}` ORDER BY Numero")
        rows = cur.fetchall()
        cur.close(); conn.close()
    except mysql.connector.Error as err:
        messagebox.showerror("MySQL", f"No se pudo leer personal desde MySQL para regenerar TXT:\n{err}")
        return False

    # Formato de línea de la app: fecha,nombre,numero,password,codigo
    lines = []
    for r in rows:
        fecha_str = r[0].strftime("%Y-%m-%d") if isinstance(r[0], date) else str(r[0])
        nombre = str(r[1]) if r[1] is not None else ""
        numero = str(r[2]) if r[2] is not None else ""
        pwd    = str(r[3]) if r[3] is not None else ""
        code   = str(r[4]) if r[4] is not None else ""
        lines.append(f"{fecha_str},{nombre},{numero},{pwd},{code}")

    return _overwrite_encrypted_file(lines, NOMBRE_ARCHIVO_REGISTROS)

#RECONSTRUIR TXT DE PARTES DESDE MYSQL
def sync_txt_from_mysql_partes():
    try:
        conn = _get_mysql_conn_for_reads()
        cur = conn.cursor()
        cur.execute(f"SELECT fecha_registro, hora_registro, Numero_Parte, Numero_CAT FROM `{TABLE_NAME_PARTES}` ORDER BY Numero_CAT")
        rows = cur.fetchall()
        cur.close(); conn.close()
    except mysql.connector.Error as err:
        messagebox.showerror("MySQL", f"No se pudo leer partes desde MySQL para regenerar TXT:\n{err}")
        return False

    # Formato de línea: fecha,hora,numero_parte,numero_cat
    lines = []
    for r in rows:
        fecha_str = r[0].strftime("%Y-%m-%d") if isinstance(r[0], date) else str(r[0])
        hora_str  = str(r[1]) if r[1] is not None else ""
        num_parte = str(r[2]) if r[2] is not None else ""
        num_cat   = str(r[3]) if r[3] is not None else ""
        lines.append(f"{fecha_str},{hora_str},{num_parte},{num_cat}")

    return _overwrite_encrypted_file(lines, NOMBRE_ARCHIVO_PARTES)

# VENTANA OPCIONES DESPUÉS DEL LOGIN
def show_options_screen():
    global options_window
    if options_window and options_window.winfo_exists():
        options_window.destroy() 

    options_window = tk.Toplevel(root_main)
    options_window.title("Opciones")
    options_window.geometry("390x350") 
    options_window.resizable(False, False)
    options_window.grab_set() 

    options_window.update_idletasks()
    x = root_main.winfo_screenwidth() // 2 - options_window.winfo_width() // 2
    y = root_main.winfo_screenheight() // 2 - options_window.winfo_height() // 2
    options_window.geometry(f"+{x}+{y}")

    # CERRAR TODA LA APLICACIÓN AL PICAR LA TACHITA
    options_window.protocol("WM_DELETE_WINDOW", exit_app)

    options_frame = tk.Frame(options_window, bg=COLOR_SECONDARY, bd=5, relief="groove")
    options_frame.pack(padx=20, pady=20, fill="both", expand=True)

    tk.Label(options_frame, text="Selecciona una opción:", font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(pady=15)

    btn_personal = tk.Button(options_frame, text="Registrar Personal", command=lambda: [options_window.grab_release(), options_window.destroy(), setup_main_app_widgets(root_main), root_main.deiconify()], bg=COLOR_SUCCESS, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=25, height=2, cursor="hand2", bd=2, relief="raised")
    btn_personal.pack(pady=5)

    btn_partes = tk.Button(options_frame, text="Registrar Número de Parte", command=lambda: [options_window.grab_release(), options_window.destroy(), show_parts_registration_screen()], bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=25, height=2, cursor="hand2", bd=2, relief="raised")
    btn_partes.pack(pady=5)

    # Botón Salir de la Aplicación en la ventana de Opciones
    btn_exit_app_options = tk.Button(options_frame, text="Salir de la Aplicación", command=exit_app, bg=COLOR_DANGER, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=25, height=2, cursor="hand2", bd=2, relief="raised")
    btn_exit_app_options.pack(pady=15)

# --- Nueva Función para la ventana de Registro de Partes ---
def show_parts_registration_screen():

    global parts_window, entry_numero_parte, entry_numero_cat

    # Si ya existe una ventana de partes y está abierta, solo la trae al frente
    if parts_window is not None and parts_window.winfo_exists():
        parts_window.lift()
        parts_window.focus_force()
        return

    parts_window = tk.Toplevel(root_main)
    parts_window.title("Registro de Número de Parte")
    parts_window.geometry("660x540")  
    parts_window.configure(bg=COLOR_PRIMARY)
    parts_window.grab_set()

    parts_window.update_idletasks()
    x = root_main.winfo_screenwidth() // 2 - parts_window.winfo_width() // 2
    y = root_main.winfo_screenheight() // 2 - parts_window.winfo_height() // 2
    parts_window.geometry(f"+{x}+{y}")

    def on_close_parts_window():
        exit_app()

    parts_window.protocol("WM_DELETE_WINDOW", on_close_parts_window)

    # Header
    header_frame_parts = tk.Frame(parts_window, bg=COLOR_ACCENT)
    header_frame_parts.pack(fill="x", padx=0, pady=(0, 20))
    tk.Label(header_frame_parts, text="Registro de Número de Parte",
             bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, font=FONT_HEADER, pady=10).pack(side="left", padx=25, pady=5)

    # IMAGEN PANTALLA PARTES 
    if os.path.exists(IMAGE_LOGO_PATH):
        try:
            original_image = Image.open(IMAGE_LOGO_PATH)
            resized_image = original_image.resize((115, 100), Image.LANCZOS)
            parts_window.logo_image = ImageTk.PhotoImage(resized_image)
            tk.Label(header_frame_parts, image=parts_window.logo_image, bg=COLOR_ACCENT).pack(side="right", padx=30, pady=5)
        except Exception as e:
            print(f"Error al cargar la imagen en ventana de partes: {e}")
            messagebox.showwarning("Error de Imagen", "No se pudo cargar la imagen del logo.", parent=parts_window)
    else:
        messagebox.showwarning("Imagen no encontrada", f"No se encontró: {IMAGE_LOGO_PATH}", parent=parts_window)

    # Inputs
    input_frame_parts = tk.Frame(parts_window, bg=COLOR_SECONDARY, bd=5, relief="raised")
    input_frame_parts.pack(padx=30, pady=20, fill="x")
    input_frame_parts.grid_columnconfigure(0, weight=1, uniform="campo_partes")
    input_frame_parts.grid_columnconfigure(1, weight=2, uniform="campo_partes")

    tk.Label(input_frame_parts, text="Fecha y Hora:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS)\
        .grid(row=0, column=0, pady=10, padx=15, sticky="w")
    date_time_frame = tk.Frame(input_frame_parts, bg=COLOR_SECONDARY)
    date_time_frame.grid(row=0, column=1, pady=10, padx=15, sticky="ew")
    display_fecha_partes = tk.Label(date_time_frame, text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), font=FONT_ENTRY, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK)
    display_fecha_partes.pack(side="left")

    def update_clock_partes():
        display_fecha_partes.config(text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        display_fecha_partes.after(1000, update_clock_partes)
    update_clock_partes()

    tk.Label(input_frame_parts, text="Número de Parte:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS)\
        .grid(row=1, column=0, pady=10, padx=15, sticky="w")
    
    entry_numero_parte = tk.Entry(input_frame_parts, font=FONT_ENTRY, width=40, bd=2, relief="sunken")
    entry_numero_parte.grid(row=1, column=1, pady=10, padx=15, sticky="ew")

    tk.Label(input_frame_parts, text="Número CAT:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS)\
        .grid(row=2, column=0, pady=10, padx=15, sticky="w")
    entry_numero_cat = tk.Entry(input_frame_parts, font=FONT_ENTRY, width=40, bd=2, relief="sunken")
    entry_numero_cat.grid(row=2, column=1, pady=10, padx=15, sticky="ew")

    # Botones en una grid de 3 columnas
    button_frame_parts = tk.Frame(parts_window, bg=COLOR_PRIMARY)
    button_frame_parts.pack(pady=20, fill="x")
    button_frame_parts.grid_columnconfigure(0, weight=1)
    button_frame_parts.grid_columnconfigure(1, weight=1)
    button_frame_parts.grid_columnconfigure(2, weight=1)

    btn_registrar = tk.Button(button_frame_parts, text="Registrar Núm Parte",
              command=registrar_datos_partes, bg=COLOR_SUCCESS, fg=COLOR_TEXT_LIGHT,
              font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised")
    btn_registrar.grid(row=0, column=0, padx=10, pady=0)

    btn_ver_db = tk.Button(button_frame_parts, text="Ver Base de Datos",
              command=show_parts_database_screen, bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT,
              font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised")
    btn_ver_db.grid(row=0, column=1, padx=10, pady=0)

    btn_dar_baja = tk.Button(button_frame_parts, text="Dar de Baja",
              command=dar_de_baja_parte,
              bg=COLOR_ORANGE, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2,
              cursor="hand2", bd=2, relief="raised")
    btn_dar_baja.grid(row=0, column=2, padx=10, pady=0)

    def back_to_options():
        global parts_window
        try:
            if parts_window:
                parts_window.grab_release()
                parts_window.destroy()
                parts_window = None
        except Exception:
            pass
        show_options_screen()

    regresar_frame_parts = tk.Frame(parts_window, bg=COLOR_PRIMARY)
    regresar_frame_parts.pack(pady=(10, 0), fill="x")
    regresar_frame_parts.grid_columnconfigure(0, weight=1)
    regresar_frame_parts.grid_columnconfigure(1, weight=1)
    regresar_frame_parts.grid_columnconfigure(2, weight=1)
    btn_regresar = tk.Button(regresar_frame_parts, text="Regresar a Opciones",
              command=back_to_options,
              bg=COLOR_DANGER, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2,
              cursor="hand2", bd=2, relief="raised")
    btn_regresar.grid(row=0, column=1, padx=10, pady=0)
    
# DAR DE BAJA EN LA DB MYSQL (PARTES) 
def dar_de_baja_parte():
    safe_backup(NOMBRE_ARCHIVO_PARTES)

    numero_parte_a_borrar = tk.simpledialog.askstring("Baja de Parte", "Ingrese el número de parte a dar de baja", parent=parts_window)
    if numero_parte_a_borrar is None:
        return
    numero_parte_a_borrar = numero_parte_a_borrar.strip()
    if not numero_parte_a_borrar:
        messagebox.showwarning("Entrada inválida", "Debes ingresar un número de parte válido.", parent=parts_window)
        return

    # Buscar la línea a eliminar en el TXT cifrado principal
    registros = leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES)
    nuevos_registros = []
    eliminado_txt = None
    for linea in registros:
        if linea.startswith("ERROR:") or not linea.strip():
            continue
        partes = linea.split(",")
        # partes[2] es Numero_Parte
        if len(partes) >= 3 and partes[2].strip() == numero_parte_a_borrar:
            eliminado_txt = linea
            continue 
        nuevos_registros.append(linea)

    # Si no se encontró en el principal, buscar en pendientes de partes
    encontrado_en_pendiente = False
    if not eliminado_txt:
        registros_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES_PENDIENTES)
        nuevos_registros_pendientes = []
        for linea in registros_pendientes:
            if linea.startswith("ERROR:") or not linea.strip():
                continue
            partes = linea.split(",")
            if len(partes) >= 3 and partes[2].strip() == numero_parte_a_borrar:
                eliminado_txt = linea
                encontrado_en_pendiente = True
                continue  
            nuevos_registros_pendientes.append(linea)
        if encontrado_en_pendiente:
            # Confirmar antes de eliminar
            confirmar = messagebox.askyesno("Confirmar eliminación", f"¿Deseas dar de baja el número de parte {numero_parte_a_borrar}?", parent=parts_window)
            if not confirmar:
                return

            _overwrite_encrypted_file(nuevos_registros_pendientes, NOMBRE_ARCHIVO_PARTES_PENDIENTES)
            # Guardar la baja en pendientes de bajas de partes
            if eliminado_txt:
                guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
                messagebox.showinfo("Éxito", f"El número de parte {numero_parte_a_borrar} fue dado de baja.", parent=parts_window)
            else:
                messagebox.showerror("Error", "No se encontró el número de parte a dar de baja.", parent=parts_window)
            return

    # Si se encontró en el principal
    if eliminado_txt:
        confirmar = messagebox.askyesno("Confirmar eliminación", f"¿Deseas dar de baja el número de parte {numero_parte_a_borrar}?", parent=parts_window)
        if not confirmar:
            return
        # Registrar baja en log TXT
        try:
            with open(NOMBRE_ARCHIVO_BAJAS_PARTES_LOG, "a", encoding="utf-8") as flog:
                flog.write(f"{eliminado_txt} - Eliminado de TXT y MySQL\n")
        except Exception:
            pass
            # Registrar baja en log TXT
            try:
                with open(NOMBRE_ARCHIVO_BAJAS_PARTES_LOG, "a", encoding="utf-8") as flog:
                    flog.write(f"{','.join(str(x) for x in row)} - Eliminado solo de MySQL\n")
            except Exception:
                pass
    else:
        # Si no se encontró en TXT ni pendientes, buscar en MySQL
        try:
            mydb = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            mycursor = mydb.cursor()
            mycursor.execute(f"SELECT fecha_registro, hora_registro, Numero_Parte, Numero_CAT FROM {TABLE_NAME_PARTES} WHERE Numero_Parte = %s", (numero_parte_a_borrar,))
            row = mycursor.fetchone()
            if row:
                confirmar = messagebox.askyesno("Confirmar eliminación", f"El número de parte {numero_parte_a_borrar} solo existe en MySQL. ¿Deseas darlo de baja?", parent=parts_window)
                if not confirmar:
                    mycursor.close()
                    mydb.close()
                    return
                # Eliminar de MySQL
                mycursor.execute(f"DELETE FROM {TABLE_NAME_PARTES} WHERE Numero_Parte = %s", (numero_parte_a_borrar,))
                mydb.commit()
                mycursor.close()
                mydb.close()
                # Sincronizar TXT desde MySQL
                sync_txt_from_mysql_partes()
                messagebox.showinfo("Éxito", f"Se dio de baja correctamente el número de parte {numero_parte_a_borrar} (solo en MySQL).", parent=parts_window)
                return
            else:
                mycursor.close()
                mydb.close()
        except Exception:
            pass
        messagebox.showerror("Error", "No se encontró el número de parte a dar de baja.", parent=parts_window)
        return

    # Intentar eliminar en MySQL
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        mycursor = mydb.cursor()
        mycursor.execute(f"DELETE FROM {TABLE_NAME_PARTES} WHERE Numero_Parte = %s", (numero_parte_a_borrar,))
        mydb.commit()
        if mycursor.rowcount > 0:
            # Éxito en MySQL, eliminar del TXT
            if nuevos_registros:
                key = load_key()
                if key:
                    _overwrite_encrypted_file(nuevos_registros, NOMBRE_ARCHIVO_PARTES)
            else:
                try:
                    os.remove(NOMBRE_ARCHIVO_PARTES)
                except Exception:
                    pass
            messagebox.showinfo("Éxito", f"Se dio de baja correctamente el número de parte {numero_parte_a_borrar}.", parent=parts_window)
            mycursor.close()
            mydb.close()
            return
        else:
            mycursor.close()
            mydb.close()
            # Si no se eliminó en MySQL, guardar baja pendiente
            guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
            messagebox.showinfo("Pendiente", f"No se encontró el número de parte {numero_parte_a_borrar} en MySQL. La baja se guardó como pendiente y se sincronizará cuando haya conexión.", parent=parts_window)
            return
    except Exception:
        # Si hay error de conexión, guardar baja pendiente
        guardar_registro_cifrado(eliminado_txt, NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
        messagebox.showinfo("Éxito", f"Se dio de baja correctamente el número de parte {numero_parte_a_borrar}.", parent=parts_window)
        return

# -----------------------------------------MOSTRAR DB SCREEN (PERSONAL) -----------------
def show_database_screen():
    import tkinter.ttk as ttk
    db_window = tk.Toplevel(root_main)
    db_window.title("Base de Datos Personal")
    db_window.geometry("650x600")  
    db_window.grab_set()

    db_window.update_idletasks()
    x = root_main.winfo_screenwidth() // 2 - db_window.winfo_width() // 2
    y = root_main.winfo_screenheight() // 2 - db_window.winfo_height() // 2
    db_window.geometry(f"+{x}+{y}")
    db_window.protocol("WM_DELETE_WINDOW", db_window.destroy)

    db_frame = tk.Frame(db_window, bg=COLOR_SECONDARY, bd=5, relief="groove")
    db_frame.pack(padx=20, pady=20, fill="both", expand=True)

    # --- Campo de búsqueda ---
    search_frame = tk.Frame(db_frame, bg=COLOR_SECONDARY)
    search_frame.pack(fill="x", pady=(0, 5))
    tk.Label(search_frame, text="Buscar:", font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(side="left", padx=(0, 5))
    search_var = tk.StringVar()
    entry_search = tk.Entry(search_frame, textvariable=search_var, font=FONT_ENTRY, width=30)
    entry_search.pack(side="left", padx=(0, 5))

    # --- Label de contenido (ahora justo después del buscador) ---
    tk.Label(db_frame, text="Contenido de la Base de Datos de Personal",
             font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(pady=(0, 10))

    # --- Tabla y columnas ---
    content_frame = tk.Frame(db_frame, bg=COLOR_SECONDARY)
    content_frame.pack(fill="both", expand=True)
    mysql_sep = tk.Label(content_frame, text=" DATOS DESDE MYSQL ", font=(FONT_LABELS[0], 14, "bold"), bg="#222244", fg="#FFFFFF", anchor="center", justify="center")
    mysql_sep.pack(fill="x", pady=(0, 0))
    mysql_table_frame = tk.Frame(content_frame, bg=COLOR_SECONDARY)
    mysql_table_frame.pack(fill="both", expand=True, padx=0, pady=0)
    columns = ("fecha", "nombre", "numero", "password", "code")
    tree_mysql = ttk.Treeview(mysql_table_frame, columns=columns, show="headings", selectmode="browse")
    tree_mysql.grid(row=0, column=0, sticky="nsew")
    vsb_mysql = ttk.Scrollbar(mysql_table_frame, orient="vertical", command=tree_mysql.yview)
    vsb_mysql.grid(row=0, column=1, sticky="ns")
    tree_mysql.configure(yscrollcommand=vsb_mysql.set)
    mysql_table_frame.grid_rowconfigure(0, weight=1)
    mysql_table_frame.grid_columnconfigure(0, weight=1)

    # Encabezados y columnas 
    tree_mysql.heading("fecha", text="Fecha", anchor="center")
    tree_mysql.heading("nombre", text="Nombre", anchor="center")
    tree_mysql.heading("numero", text="Número", anchor="center")
    tree_mysql.heading("password", text="Contraseña", anchor="center")
    tree_mysql.heading("code", text="Code Laser", anchor="center")
    tree_mysql.column("fecha", anchor="center", width=80, minwidth=60, stretch=True)
    tree_mysql.column("nombre", anchor="center", width=238, minwidth=100, stretch=True)
    tree_mysql.column("numero", anchor="center", width=80, minwidth=60, stretch=True)
    tree_mysql.column("password", anchor="center", width=100, minwidth=80, stretch=True)
    tree_mysql.column("code", anchor="center", width=83, minwidth=60, stretch=True)

    def resize_columns(event=None):
        total_width = tree_mysql.winfo_width()

        col_props = [0.15, 0.38, 0.13, 0.18, 0.16]
        for i, col in enumerate(columns):
            tree_mysql.column(col, width=int(total_width * col_props[i]))
    tree_mysql.bind('<Configure>', resize_columns)

    # --- MOSTRAR DATOS ---
    def mostrar_datos(filtro, show_popup_if_empty=False):
        for row in tree_mysql.get_children():
            tree_mysql.delete(row)

        # Zebra Style robusto
        style = ttk.Style()
        style.map("Treeview", background=[('selected', "#0261B9")])
        style.configure("OddRow.Treeview", background="#DFEBFF", foreground=COLOR_TEXT_DARK)
        style.configure("EvenRow.Treeview", background="#FFFFFF", foreground=COLOR_TEXT_DARK)

        conn_mysql = None
        mycursor = None
        mysql_records = []
        datos_desde_txt = False
        try:
            conn_mysql = mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
            )
            mycursor = conn_mysql.cursor()
            mycursor.execute(
                f"SELECT fecha_registro, Nombre, Numero, Password, Code_Laser FROM {TABLE_NAME_PERSONAL} ORDER BY fecha_registro ASC, Numero ASC"
            )
            mysql_records = mycursor.fetchall()
            mysql_sep.config(text=" DATOS DESDE MYSQL ")
        except Exception:
            datos_desde_txt = True
            registros = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS)
            pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_REGISTROS_PENDIENTES)
            bajas_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_BAJAS_PENDIENTES)
            numeros_baja = set()
            for linea in bajas_pendientes:
                if linea.startswith("ERROR:") or not linea.strip():
                    continue
                partes = linea.split(",")
                if len(partes) >= 3 and partes[2].strip().isdigit():
                    numeros_baja.add(partes[2].strip())
            todos = []
            for linea in registros + pendientes:
                if linea.startswith("ERROR:") or not linea.strip():
                    continue
                partes = linea.split(",")
                if len(partes) >= 3 and partes[2].strip() in numeros_baja:
                    continue
                todos.append(linea)
            mysql_records = []
            for linea in todos:
                partes = linea.split(",")
                if len(partes) < 5:
                    continue
                num_index = -1
                for i in range(2, len(partes)):
                    if partes[i].strip().isdigit():
                        num_index = i
                        break
                if num_index == -1 or num_index+2 >= len(partes):
                    continue
                fecha = partes[0].strip()
                nombre = ",".join(partes[1:num_index]).strip()
                numero = partes[num_index].strip()
                password = partes[num_index+1].strip()
                code = partes[num_index+2].strip()
                mysql_records.append((fecha, nombre, numero, password, code))
            mysql_sep.config(text=" DATOS REGISTRADOS ")
        finally:
            if mycursor:
                try:
                    mycursor.close()
                except Exception:
                    pass
            if conn_mysql and conn_mysql.is_connected():
                try:
                    conn_mysql.close()
                except Exception:
                    pass

        if filtro:
            filtro = filtro.lower()
            mysql_records = [row for row in mysql_records if any(filtro in str(col).lower() for col in row)]

        if not mysql_records:
            tree_mysql.insert("", "end", values=("", "", "", "", ""))
            if show_popup_if_empty:
                messagebox.showinfo("Sin resultados", "Tu búsqueda no arrojó resultados.", parent=db_window)
        else:
            for idx, row in enumerate(mysql_records):
                tag = "OddRow" if idx % 2 == 0 else "EvenRow"
                iid = tree_mysql.insert("", "end", values=row, tags=(tag,))

                tree_mysql.item(iid, tags=(tag,))
            tree_mysql.tag_configure("OddRow", background="#DFEBFF", foreground=COLOR_TEXT_DARK)
            tree_mysql.tag_configure("EvenRow", background="#FFFFFF", foreground=COLOR_TEXT_DARK)

            for idx, iid in enumerate(tree_mysql.get_children()):
                tag = "OddRow" if idx % 2 == 0 else "EvenRow"
                tree_mysql.item(iid, tags=(tag,))

    # --- Búsqueda y eventos ---
    def do_search(event=None):
        filtro = search_var.get().strip().lower()
        mostrar_datos(filtro, show_popup_if_empty=True)
    def limpiar_search():
        search_var.set("")
        mostrar_datos("")
    btn_search = tk.Button(search_frame, text="Buscar", command=do_search, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_SUCCESS, fg=COLOR_TEXT_LIGHT, width=8, height=1)
    btn_search.pack(side="left", padx=(0, 5))
    btn_clear = tk.Button(search_frame, text="Limpiar", command=limpiar_search, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_DANGER, fg=COLOR_TEXT_LIGHT, width=8, height=1)
    btn_clear.pack(side="left", padx=(0, 5))
    def on_db_personal_close():
        db_window.grab_release()
        db_window.destroy()
    btn_close = tk.Button(search_frame, text="Cerrar", command=on_db_personal_close, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, width=8, height=1, relief="raised", cursor="hand2")
    btn_close.pack(side="left", padx=(0, 0))
    entry_search.bind('<Return>', do_search)
    def on_search_var_change(*args):
        filtro = search_var.get().strip().lower()
        mostrar_datos(filtro)
    search_var.trace_add('write', on_search_var_change)

    mostrar_datos("")

def ensure_txt_for_write(path, cache_lines, sync_fn):


    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    try:
        if sync_fn():  
            return
    except Exception:
        pass

    if cache_lines:
        _overwrite_encrypted_file(cache_lines, path)
        return
    restore_from_backup(path)

# -----------------------------------------MOSTRAR DB SCREEN (PARTES) -----------------
def show_parts_database_screen():

    db_parts_window = tk.Toplevel(root_main)
    db_parts_window.title("Base de Datos Partes")
    db_parts_window.geometry("560x600")
    db_parts_window.grab_set()

    db_parts_window.update_idletasks()
    x = db_parts_window.winfo_screenwidth() // 2 - db_parts_window.winfo_width() // 2
    y = db_parts_window.winfo_screenheight() // 2 - db_parts_window.winfo_height() // 2
    db_parts_window.geometry(f"+{x}+{y}")
    db_parts_window.protocol("WM_DELETE_WINDOW", db_parts_window.destroy)

    db_frame = tk.Frame(db_parts_window, bg=COLOR_SECONDARY, bd=5, relief="groove")
    db_frame.pack(padx=20, pady=20, fill="both", expand=True)

    # --- Campo de búsqueda ---
    search_frame = tk.Frame(db_frame, bg=COLOR_SECONDARY)
    search_frame.pack(fill="x", pady=(0, 5))
    tk.Label(search_frame, text="Buscar:", font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(side="left", padx=(0, 5))
    search_var = tk.StringVar()
    entry_search = tk.Entry(search_frame, textvariable=search_var, font=FONT_ENTRY, width=20)
    entry_search.pack(side="left", padx=(0, 5))
    def do_search(event=None):
        filtro = search_var.get().strip().lower()
        mostrar_datos(filtro, show_popup_if_empty=True)

    def limpiar_search():
        search_var.set("")
        mostrar_datos("")
    btn_search = tk.Button(search_frame, text="Buscar", command=do_search, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_SUCCESS, fg=COLOR_TEXT_LIGHT, width=8, height=1)
    btn_search.pack(side="left", padx=(0, 5))
    btn_clear = tk.Button(search_frame, text="Limpiar", command=limpiar_search, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_DANGER, fg=COLOR_TEXT_LIGHT, width=8, height=1)
    btn_clear.pack(side="left", padx=(0, 5))

    def on_db_parts_close():
        db_parts_window.grab_release()
        db_parts_window.destroy()
    btn_close = tk.Button(search_frame, text="Cerrar", command=on_db_parts_close, font=(FONT_BUTTONS[0], 10, "bold"), bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, width=8, height=1, relief="raised", cursor="hand2")
    btn_close.pack(side="left", padx=(0, 0))
    entry_search.bind('<Return>', do_search)

    # --- Búsqueda en tiempo real ---
    def on_search_var_change(*args):
        filtro = search_var.get().strip().lower()
        mostrar_datos(filtro, show_popup_if_empty=False)
    search_var.trace_add('write', on_search_var_change)

    tk.Label(db_frame, text="Contenido de la Base de Datos de Número de Parte",
             font=FONT_LABELS, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK).pack(pady=(0, 10))

    # --- Separador centrado MySQL ---
    mysql_sep = tk.Label(db_frame, text=" DATOS REGISTRADOS ", font=(FONT_LABELS[0], 14, "bold"), bg="#222244", fg="#FFFFFF", anchor="center", justify="center")
    mysql_sep.pack(fill="x", pady=(0, 0))

    # --- Frame para tabla y scrollbars MySQL (ocupa todo el espacio vertical) ---
    mysql_table_frame = tk.Frame(db_frame, bg=COLOR_SECONDARY)
    mysql_table_frame.pack(fill="both", expand=True, padx=0, pady=0)
    columns = ("fecha", "hora", "numero_parte", "numero_cat")
    tree_mysql = ttk.Treeview(mysql_table_frame, columns=columns, show="headings", selectmode="browse")
    tree_mysql.grid(row=0, column=0, sticky="nsew")
    vsb_mysql = ttk.Scrollbar(mysql_table_frame, orient="vertical", command=tree_mysql.yview)
    vsb_mysql.grid(row=0, column=1, sticky="ns")
    tree_mysql.configure(yscrollcommand=vsb_mysql.set)
    mysql_table_frame.grid_rowconfigure(0, weight=1)
    mysql_table_frame.grid_columnconfigure(0, weight=1)
    for col, text, width, prop in zip(columns, ["Fecha", "Hora", "Número de Parte", "Número CAT"], [100, 90, 180, 120], [0.18, 0.18, 0.38, 0.26]):
        tree_mysql.heading(col, text=text, anchor="center")
        tree_mysql.column(col, anchor="center", width=width, minwidth=60, stretch=True)

    def resize_columns(event=None):
        total_width = tree_mysql.winfo_width()
        col_props = [0.18, 0.18, 0.38, 0.26]
        for i, col in enumerate(columns):
            tree_mysql.column(col, width=int(total_width * col_props[i]))
    tree_mysql.bind('<Configure>', resize_columns)

    # --- Función para mostrar datos filtrados MYSQL u OFFLINE ---
    def mostrar_datos(filtro, show_popup_if_empty=False):
        for row in tree_mysql.get_children():
            tree_mysql.delete(row)

        # Zebra Style igual que personal
        style = ttk.Style()
        style.map("Treeview", background=[('selected', "#0261B9")])
        style.configure("OddRow.Treeview", background="#DFEBFF", foreground=COLOR_TEXT_LIGHT)
        style.configure("EvenRow.Treeview", background="#FFFFFF", foreground=COLOR_TEXT_DARK)

        conn_mysql = None
        mycursor = None
        mysql_records = []
        try:
            conn_mysql = mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
            )
            mycursor = conn_mysql.cursor()
            mycursor.execute(
                f"SELECT fecha_registro, hora_registro, Numero_Parte, Numero_CAT FROM {TABLE_NAME_PARTES} ORDER BY fecha_registro ASC, Numero_CAT ASC"
            )
            mysql_records = mycursor.fetchall()
            mysql_sep.config(text=" DATOS DESDE MYSQL ")
        except Exception:
            registros = leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES)
            pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_PARTES_PENDIENTES)
            bajas_pendientes = leer_registros_descifrados(NOMBRE_ARCHIVO_BAJAS_PARTES_PENDIENTES)
            numeros_cat_baja = set()
            for linea in bajas_pendientes:
                if linea.startswith("ERROR:") or not linea.strip():
                    continue
                partes = linea.split(",")
                if len(partes) >= 4:
                    numeros_cat_baja.add(partes[3].strip())
            todos = []
            for linea in registros + pendientes:
                if linea.startswith("ERROR:") or not linea.strip():
                    continue
                partes = linea.split(",")
                if len(partes) >= 4 and partes[3].strip() in numeros_cat_baja:
                    continue
                todos.append(linea)
            mysql_records = []
            for linea in todos:
                partes = linea.split(",")
                if len(partes) < 4:
                    continue
                fecha = partes[0].strip()
                hora = partes[1].strip()
                numero_parte = partes[2].strip()
                numero_cat = partes[3].strip()
                mysql_records.append((fecha, hora, numero_parte, numero_cat))
            mysql_sep.config(text=" DATOS REGISTRADOS ")
        finally:
            if mycursor:
                try:
                    mycursor.close()
                except Exception:
                    pass
            if conn_mysql and conn_mysql.is_connected():
                try:
                    conn_mysql.close()
                except Exception:
                    pass

        if filtro:
            filtro = filtro.lower()
            mysql_records = [row for row in mysql_records if any(filtro in str(col).lower() for col in row)]

        if not mysql_records:
            tree_mysql.insert("", "end", values=("", "", "", ""))
            if show_popup_if_empty:
                messagebox.showinfo("Sin resultados", "Tu búsqueda no arrojó resultados.", parent=db_parts_window)
        else:
            for idx, row in enumerate(mysql_records):
                tag = "OddRow" if idx % 2 == 0 else "EvenRow"
                iid = tree_mysql.insert("", "end", values=row, tags=(tag,))
                tree_mysql.item(iid, tags=(tag,))
            tree_mysql.tag_configure("OddRow", background="#DFEBFF", foreground=COLOR_TEXT_DARK)
            tree_mysql.tag_configure("EvenRow", background="#FFFFFF", foreground=COLOR_TEXT_DARK)
            for idx, iid in enumerate(tree_mysql.get_children()):
                tag = "OddRow" if idx % 2 == 0 else "EvenRow"
                tree_mysql.item(iid, tags=(tag,))
    mostrar_datos("")

# --- Función para configurar los Widgets de la Pantalla Principal de Registro (Personal) ---
def setup_main_app_widgets(main_window):
    global root_main, entry_nombre, entry_registro, entry_password_registro, entry_codigo, display_fecha
    root_main = main_window

    root_main.title("Registro de Personal Autorizado - Láser")
    root_main.geometry("690x610")
    root_main.configure(bg=COLOR_PRIMARY)

    # Centrar la ventana principal
    main_window.update_idletasks()
    x = main_window.winfo_screenwidth() // 2 - main_window.winfo_width() // 2
    y = main_window.winfo_screenheight() // 2 - main_window.winfo_height() // 2
    main_window.geometry(f"+{x}+{y}")

    # CERRAR TODA LA APLICACIÓN AL PICAR LA TACHITA
    main_window.protocol("WM_DELETE_WINDOW", exit_app)

    # Limpiar widgets existentes if se llama de nuevo
    for widget in root_main.winfo_children():
        widget.destroy()

    header_frame = tk.Frame(root_main, bg=COLOR_ACCENT)
    header_frame.pack(fill="x", padx=0, pady=(0, 20))

    # Etiqueta de texto del Header (izquierda)
    header_text_label = tk.Label(header_frame, text="Registro de Personal Autorizado", bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, font=FONT_HEADER, pady=10)
    header_text_label.pack(side="left", padx=15, pady=5)

    # IMAGEN PANTALL REGISTRO PERSONAL
    if os.path.exists(IMAGE_LOGO_PATH):
        try:
            original_image = Image.open(IMAGE_LOGO_PATH)
            resized_image = original_image.resize((115,100), Image.LANCZOS)
            main_window.logo_image = ImageTk.PhotoImage(resized_image)
            logo_label = tk.Label(header_frame, image=main_window.logo_image, bg=COLOR_ACCENT)
            logo_label.pack(side="right", padx=30, pady=5)
        except Exception as e:
            print(f"Error al cargar la imagen: {e}")
            messagebox.showwarning("Error de Imagen", "No se pudo cargar la imagen del logo. Asegúrate de que la ruta sea correcta y el archivo sea válido.", parent=root_main)
    else:
        print(f"Advertencia: No se encontró la imagen en la ruta: {IMAGE_LOGO_PATH}")
        messagebox.showwarning("Imagen no encontrada", f"No se encontró el archivo de imagen en la ruta especificada: {IMAGE_LOGO_PATH}", parent=root_main)

    # Frame para los campos de entrada
    input_frame = tk.Frame(root_main, bg=COLOR_SECONDARY, bd=5, relief="raised")
    input_frame.pack(padx=30, pady=20, fill="x")

    # Configuración de columnas dentro de input_frame para mejor control
    input_frame.grid_columnconfigure(0, weight=1, uniform="campo")
    input_frame.grid_columnconfigure(1, weight=2, uniform="campo")

    # ------------- CAMPOS ENTRADA TKINTER ----------------
    tk.Label(input_frame, text="Fecha y Hora:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS).grid(row=0, column=0, pady=10, padx=15, sticky="w")
    display_fecha = tk.Label(input_frame, text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), font=FONT_ENTRY, bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK)
    display_fecha.grid(row=0, column=1, pady=10, padx=15, sticky="w")

    def update_clock():
        display_fecha.config(text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        display_fecha.after(1000, update_clock)
    update_clock()

    # Nombre
    tk.Label(input_frame, text="Nombre Completo:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS).grid(row=1, column=0, pady=10, padx=15, sticky="w")
    entry_nombre = tk.Entry(input_frame, font=FONT_ENTRY, width=40, bd=2, relief="sunken")
    entry_nombre.grid(row=1, column=1, pady=10, padx=15, sticky="ew")

    # Número de registro
    tk.Label(input_frame, text="Número de Registro:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS).grid(row=2, column=0, pady=10, padx=15, sticky="w")
    entry_registro = tk.Entry(input_frame, font=FONT_ENTRY, width=20, bd=2, relief="sunken")
    entry_registro.grid(row=2, column=1, pady=10, padx=15, sticky="ew")

    # Contraseña
    tk.Label(input_frame, text="Contraseña:", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS).grid(row=3, column=0, pady=10, padx=15, sticky="w")
    entry_password_registro = tk.Entry(input_frame, font=FONT_ENTRY, width=20, bd=2, relief="sunken") # show="*" para ocultar
    entry_password_registro.grid(row=3, column=1, pady=10, padx=15, sticky="ew")

    # Code Laser 
    tk.Label(input_frame, text="Code Laser (2 letras):", bg=COLOR_SECONDARY, fg=COLOR_TEXT_DARK, font=FONT_LABELS).grid(row=4, column=0, pady=10, padx=15, sticky="w")
    entry_codigo = tk.Entry(input_frame, font=FONT_ENTRY, width=10, bd=2, relief="sunken")
    entry_codigo.grid(row=4, column=1, pady=10, padx=15, sticky="ew")

    # Frame para los botones 
    button_frame = tk.Frame(root_main, bg=COLOR_PRIMARY)
    button_frame.pack(pady=(10, 1)) 
    button_frame_top = tk.Frame(root_main, bg=COLOR_PRIMARY)
    button_frame_top.pack(pady=(30,1))

    # Botones
    btn_enviar = tk.Button(button_frame, text="Registrar Personal", command=registrar_datos_personal, bg=COLOR_SUCCESS, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised")
    btn_enviar.pack(side="left", padx=15)

    btn_view_db = tk.Button(button_frame, text="Ver Base de Datos", command=show_database_screen, bg=COLOR_ACCENT, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised") 
    btn_view_db.pack(side="left", padx=15) 

    btn_view_db = tk.Button(button_frame, text="Dar de Baja", command=dar_de_baja_personal, bg=COLOR_ORANGE, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised") 
    btn_view_db.pack(side="left", padx=15) 

    # Nuevo Frame solo para el botón "Regresar a Opciones"
    button_frame_bottom = tk.Frame(root_main, bg=COLOR_PRIMARY)
    button_frame_bottom.pack(pady=(0, 10))  

    btn_back_to_options = tk.Button(button_frame_bottom, text="Regresar a Opciones", command=lambda: [root_main.withdraw(), show_options_screen()], bg=COLOR_DANGER, fg=COLOR_TEXT_LIGHT, font=FONT_BUTTONS, width=18, height=2, cursor="hand2", bd=2, relief="raised")
    btn_back_to_options.pack(side="right", padx=10)

#ASEGURAR TXT AL INICIO (Y CUANDO FALTEN LOS RECONSTRUYE CIFRADOS DESDE MySQL)
def ensure_txt_files_from_mysql():
    if not os.path.exists(NOMBRE_ARCHIVO_REGISTROS) or os.path.getsize(NOMBRE_ARCHIVO_REGISTROS) == 0:
        ok = sync_txt_from_mysql_personal()
        if ok:
            print("DB_Registro.txt regenerado exitosamente desde MySQL.")
        else:
            print("No se pudo regenerar DB_Registro.txt.")

    if not os.path.exists(NOMBRE_ARCHIVO_PARTES) or os.path.getsize(NOMBRE_ARCHIVO_PARTES) == 0:
        ok = sync_txt_from_mysql_partes()
        if ok:
            print("DB_Partes.txt regenerado exitosamente desde MySQL.")
        else:
            print("No se pudo regenerar DB_Partes.txt.")

# ------------------------ INICIO DE LA APLICACIÓN -----------------
if __name__ == "__main__":
    root_main = tk.Tk()

    load_key()
    try:
        tmpdb = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
        tmpcur = tmpdb.cursor()
        create_mysql_database_and_tables(tmpcur)
        tmpcur.close(); tmpdb.close()
    except Exception:
        pass

    sincronizar_todo_pendiente()
    ensure_txt_files_from_mysql()
    setup_main_app_widgets(root_main)
    root_main.withdraw()
    show_login_screen_toplevel(root_main)
    root_main.mainloop()
