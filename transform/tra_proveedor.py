import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_proveedor():

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

        # Consulta SQL para realizar la transformación de la tabla tra_Proveedor
        sql_stmt = """
            SELECT
                p.ID_Proveedor,
                p.Nombre,
                p.Ubicacion,
                p.ContactoNombre,
                p.ContactoEmail,
                p.ContactoTelefono,
                -- Columnas adicionales de tra_Info_Contratacion
                ic.Contrataciones
            FROM
                ext_Proveedor p
            LEFT JOIN
                tra_Info_Contratacion ic ON (p.ID_Proveedor = ic.ID_Proveedor)
        """

        # Realizar la transformación y obtener el DataFrame resultante
        proveedor_tra = pd.read_sql(sql_stmt, ses_db)

        return proveedor_tra

    except:
        traceback.print_exc()
    finally:
        pass
