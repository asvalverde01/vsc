import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_servicio_publicitario():

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

        # Consulta SQL para realizar la transformación de la tabla tra_Servicio_Publicitario
        sql_stmt = """
            SELECT
                sp.ID_Servicio,
                sp.Nombre,
                sp.Categoria,
                sp.Descripcion,
                sp.Tarifa,
                -- Columnas adicionales de tra_Datos_Ventas
                dv.Monto,
                dv.CantidadVentas
            FROM
                ext_Servicio_Publicitario sp
            LEFT JOIN
                tra_Datos_Ventas dv ON (sp.ID_Servicio = dv.ID_Servicio)
        """

        # Realizar la transformación y obtener el DataFrame resultante
        servicio_publicitario_tra = pd.read_sql(sql_stmt, ses_db)

        return servicio_publicitario_tra

    except:
        traceback.print_exc()
    finally:
        pass
