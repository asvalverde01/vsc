import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def cargar_dim_contratacion():

    try:
        # Configuración para la base de datos de staging
        stg_db_type = 'staging'
        con_db_stg = Db_Connection(stg_db_type)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg is None:
            raise Exception(f"Error al establecer la conexión de staging")

        # Realizar la transformación y obtener el DataFrame resultante
        contratacion_tra = pd.read_sql("SELECT * FROM tra_Contratacion", ses_db_stg)

        # Configuración para la base de datos de sor
        sor_db_type = 'sor'
        con_db_sor = Db_Connection(sor_db_type)
        ses_db_sor = con_db_sor.start()

        if ses_db_sor is None:
            raise Exception(f"Error al establecer la conexión de SOR")

        # Nombre de la tabla en la base de datos sor
        table_name_sor = 'Dim_Contratacion'

        # Renombrar columnas para evitar colisiones
        contratacion_tra = contratacion_tra.rename(columns={'Proveedor': 'Proveedor_stg', 'Dia': 'Dias_Duracion_stg'})

        # Fusionar (merge) los datos transformados en la tabla Dim_Contratacion de sor
        contratacion_tra.to_sql(table_name_sor, ses_db_sor, if_exists='replace', index=False, method='multi')

    except:
        traceback.print_exc()
    finally:
        # Cerrar las conexiones al finalizar
        if con_db_stg:
            con_db_stg.stop()
        if con_db_sor:
            con_db_sor.stop()

