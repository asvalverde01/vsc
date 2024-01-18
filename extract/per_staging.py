import traceback
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig
import pandas as pd

def persistir_staging (df_stg, tab_name, db_type='staging'):

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
        
        df_stg.to_sql(tab_name, ses_db, if_exists='replace', index=False)

    except:
        traceback.print_exc()
    finally:
        pass
