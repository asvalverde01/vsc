import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_contratacion():

    try:
        # Configuración de conexión a la base de datos
        type = 'mysql'
        host = '10.10.10.2'
        port = '3306'
        user = 'dwh'
        pwd = 'elcaro_4U'
        db = 'staging'

        # Iniciar conexión a la base de datos
        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db == -2:
            raise Exception("Error al establecer la conexión de pruebas")

        # Consulta SQL para realizar la transformación de la tabla tra_Contratacion
        sql_stmt = """
            SELECT
                c.ID_Proveedor,
                c.Area_Elevision,
                c.Fecha,
                c.Tipo_Servicio,
                c.Duracion,
                c.Comentarios,
                -- Columnas adicionales de tra_Info_Contratacion
                ic.Contrataciones
            FROM
                ext_Contratacion c
            LEFT JOIN
                tra_Info_Contratacion ic ON (c.ID_Proveedor = ic.ID_Proveedor AND c.Area_Elevision = ic.Area_Elevision AND c.Tipo_Servicio = ic.Tipo_Servicio)
        """

        # Realizar la transformación y obtener el DataFrame resultante
        contratacion_tra = pd.read_sql(sql_stmt, ses_db)

        return contratacion_tra

    except:
        traceback.print_exc()
    finally:
        pass

