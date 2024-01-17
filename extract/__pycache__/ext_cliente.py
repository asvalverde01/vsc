import traceback
from util.db_connection import Db_Connection
import pandas as pd

def extract_cliente():
    try:
        # Configuración de conexión
        type = 'mysql'
        host = '10.10.10.2'
        port = '3306'
        user = 'dwh'
        pwd = 'elcaro_4U'
        db = 'oltp'

        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()

        if ses_db == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db == -2:
            raise Exception("Error al establecer la conexión de pruebas")

        # Extracción de datos
        cliente_data = pd.read_sql('SELECT * FROM Cliente', ses_db)

        return cliente_data

    except:
        traceback.print_exc()
    finally:
        # Cerrar conexión
        con_db.close()

# Puedes replicar este patrón para otras tablas en archivos separados
