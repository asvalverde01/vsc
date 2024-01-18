import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def transformar_contratacion(db_type='staging'):

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

        # Consulta SQL para realizar la transformación de la tabla tra_Contratacion
        sql_stmt = """
            SELECT
                p.Nombre AS Proveedor,
                c.Area_Elevision,
                DAY(c.Fecha) AS Dia,
                MONTH(c.Fecha) AS Mes,
                YEAR(c.Fecha) AS Anio,
                c.Tipo_Servicio,
                c.Duracion AS Dias_Duracion,
                -- Columnas adicionales de tra_Info_Contratacion
                ic.Contrataciones
            FROM
                ext_Contratacion c
            LEFT JOIN
                tra_Info_Contratacion ic ON (c.ID_Proveedor = ic.ID_Proveedor AND c.Area_Elevision = ic.Area_Elevision AND c.Tipo_Servicio = ic.Tipo_Servicio)
            LEFT JOIN
                ext_Proveedor p ON c.ID_Proveedor = p.ID_Proveedor
        """

        # Realizar la transformación y obtener el DataFrame resultante
        contratacion_tra = pd.read_sql(sql_stmt, ses_db)

        return contratacion_tra

    except:
        traceback.print_exc()
        return None
