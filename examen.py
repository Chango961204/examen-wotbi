import mysql.connector

# Conexión a la base de datos
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="",
    database="bdexamen"
)

cursor = conn.cursor()

# Ruta de los archivos
rutaArchivoUnidadEconomica = r"C:\Users\metal\OneDrive\Escritorio\wotbi\data.txt"
rutaArchivoDireccion = r"C:\Users\metal\OneDrive\Escritorio\wotbi\direccion.txt"

# Función para procesar un archivo y actualizar la base de datos
def procesar_archivo(archivo, tabla):
    ids_to_delete = set()  # Inicializa la variable ids_to_delete como un conjunto vacío
    with open(archivo, "r") as archivo:
        header = archivo.readline().strip()
        column_names = header.split(",")

        print("Columnas en el archivo:", column_names)

        # Obtén la lista de todos los IDs presentes en la base de datos para la tabla actual
        cursor.execute(f"SELECT id FROM {tabla}")
        ids_in_database = set(str(row[0]) for row in cursor.fetchall())

        # Obtén la lista de todos los IDs presentes en el archivo de texto
        ids_in_file = set()

        for linea in archivo:
            linea = linea.strip()
            if not linea:
                continue
            valores = linea.split(",")
            if len(valores) != len(column_names):
                print(f"Error: La cantidad de valores en la línea no coincide con las columnas en {tabla}.")
                continue
            id_value = valores[0]
            ids_in_file.add(id_value)

        # Elimina registros de la tabla direccion que tienen la unidadEconomicaId correspondiente
        if tabla == "unidadEconomica":
            ids_to_delete = ids_in_database - ids_in_file
            if ids_to_delete:
                ids_to_delete_str = ', '.join(ids_to_delete)
                cursor.execute(f"DELETE FROM direccion WHERE unidadEconomicaId IN ({ids_to_delete_str})")

        # Elimina registros de la base de datos que no están presentes en el archivo
        if ids_to_delete:
            ids_to_delete_str = ', '.join(ids_to_delete)
            cursor.execute(f"DELETE FROM {tabla} WHERE id IN ({ids_to_delete_str})")

        archivo.seek(0)
        archivo.readline()
        for linea in archivo:
            valores = linea.strip().split(",")
            if len(valores) != len(column_names):
                print(f"Error: La cantidad de valores en la línea no coincide con las columnas en {tabla}.")
                continue

            # Verifica si ya existe un registro con el mismo valor de 'id' en la base de datos
            cursor.execute(f"SELECT id FROM {tabla} WHERE id = %s", (valores[0],))
            existing_record = cursor.fetchone()

            if existing_record:
                # Si existe, actualiza el registro en lugar de insertarlo nuevamente
                sql = f"""
                UPDATE {tabla}
                SET {" = %s, ".join(column_names[1:])} = %s
                WHERE id = %s
                """
                cursor.execute(sql, (valores[1:] + [valores[0]]))
            else:
                # Si no existe, realiza la inserción
                sql = f"""
                INSERT INTO {tabla} ({", ".join(column_names)})
                VALUES ({", ".join(['%s'] * len(column_names))})
                """
                cursor.execute(sql, valores)

        # Confirma los cambios
        conn.commit()

# Procesar archivo unidadEconomica.txt y direccion.txt
procesar_archivo(rutaArchivoUnidadEconomica, "unidadEconomica")
procesar_archivo(rutaArchivoDireccion, "direccion")

# Cierra la conexión
conn.close()
