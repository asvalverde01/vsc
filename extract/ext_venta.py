# Archivo: extract/ext_venta.py
import traceback
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig
import pandas as pd

def extract_venta(db_type='oltp'):
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

        venta_data = pd.read_sql('SELECT * FROM Venta', ses_db)

        return venta_data

    except:
        traceback.print_exc()
    finally:
        pass
