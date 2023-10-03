import mysql.connector

# Conexión a la base de datos
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="", 
    database="bdexamen"
)

cursor = conn.cursor()

rutaArchivo = r"C:\Users\metal\OneDrive\Escritorio\wotbi\data.txt"

with open(rutaArchivo, "r") as archivo:
    header = archivo.readline().strip()
    column_names = header.split(",")

    print("columnas en el archivo:", column_names)

    # Obtén la lista de todos los IDs presentes en el archivo de texto
    ids_in_file = set()

    for linea in archivo:
        linea = linea.strip()  
        if not linea:
            continue
        valores = linea.split(",")
        if len(valores) != len(column_names):
            print("Error: La cantidad de valores en la línea no coincide con las columnas.")
            print("Valores en la línea:", valores) 
            continue
        
        id_value = valores[0]
        ids_in_file.add(id_value)

    # Obtén la lista de todos los IDs presentes en la base de datos
    cursor.execute("SELECT id FROM unidadEconomica")
    db_ids = set(record[0] for record in cursor.fetchall())

    # Elimina registros de la base de datos que no están presentes en el archivo
    ids_to_delete = db_ids - ids_in_file
    if ids_to_delete:
        for id_to_delete in ids_to_delete:
            cursor.execute("DELETE FROM unidadEconomica WHERE id = %s", (id_to_delete,))

    # Inserta o actualiza registros en la base de datos desde el archivo
    archivo.seek(0)  
    archivo.readline()  
    for linea in archivo:
        valores = linea.strip().split(",")
        if len(valores) != len(column_names):
            print("Error: La cantidad de valores en la línea no coincide con las columnas.")
            continue

        # Verifica si ya existe un registro con el mismo valor de 'id' en la base de datos
        cursor.execute("SELECT id FROM unidadEconomica WHERE id = %s", (valores[0],))
        existing_record = cursor.fetchone()

        if existing_record:
            # Si existe, actualiza el registro en lugar de insertarlo nuevamente
            sql = f"""
            UPDATE unidadEconomica
            SET nombreUnidadEconomica = %s, razonSocial = %s, codigoActividadSCIAN = %s,
                nombreActividad = %s, descripcionEstratoPersonalOcupado = %s
            WHERE id = %s
            """
            cursor.execute(sql, (valores[1], valores[2], valores[3], valores[4], valores[5], valores[0]))
        else:
            # Si no existe, realiza la inserción
            sql = f"""
            INSERT INTO unidadEconomica ({", ".join(column_names)})
            VALUES ({", ".join(['%s'] * len(column_names))})
            """
            cursor.execute(sql, valores)

conn.commit()

conn.close()
