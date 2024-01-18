# Archivo: extract/ext_servicio_publicitario.py
import traceback
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig
import pandas as pd

def extract_servicio_publicitario(db_type='oltp'):
    try:
        config = DatabaseConfig.DATABASES.get(db_type)

        if config is None:
            raise Exception(f"Invalid database type: {db_type}")

        con_db = Db_Connection(db_type)
        ses_db = con_db.start()
        
        if ses_db == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db == -2:
            raise Exception("Error al establecer la conexión de pruebas")        

        servicio_publicitario_data = pd.read_sql('SELECT * FROM Servicio_Publicitario', ses_db)

        return servicio_publicitario_data

    except:
        traceback.print_exc()
    finally:
        pass
