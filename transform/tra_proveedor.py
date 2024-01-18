import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def transformar_proveedor(db_type='staging'):

    try:
        config = DatabaseConfig.DATABASES.get(db_type)

        if config is None:
            raise Exception(f"Invalid database type: {db_type}")

        # Iniciar conexión a la base de datos
        con_db = Db_Connection(db_type)
        ses_db = con_db.start()
        
        if ses_db == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db == -2:
            raise Exception("Error al establecer la conexión de pruebas")

        # Consulta SQL para realizar la transformación de la tabla tra_Proveedor
        sql_stmt = """
            SELECT
                p.Nombre AS Proveedor,
                p.Ubicacion,
                p.ContactoNombre,
                -- Columnas adicionales de tra_Info_Contratacion
                ic.Contrataciones
            FROM
                ext_Proveedor p
            LEFT JOIN
                tra_Info_Contratacion ic ON (p.ID_Proveedor = ic.ID_Proveedor)
            LEFT JOIN
                ext_Contratacion c ON (p.ID_Proveedor = c.ID_Proveedor)
            GROUP BY
                p.Nombre, p.Ubicacion, p.ContactoNombre, ic.Contrataciones
        """

        # Realizar la transformación y obtener el DataFrame resultante
        proveedor_tra = pd.read_sql(sql_stmt, ses_db)

        return proveedor_tra

    except:
        traceback.print_exc()
        return None
